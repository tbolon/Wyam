using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Wyam.Core.Modules.Metadata
{
    partial class Sql
    {
        private class DataSetExtensions
        {
            #region EnumerableRowCollection.cs

            /// <summary>
            /// Provides an entry point so that Cast operator call can be intercepted within an extension method.
            /// </summary>
            public abstract class EnumerableRowCollection : IEnumerable
            {
                internal abstract Type ElementType { get; }
                internal abstract DataTable Table { get; }

                internal EnumerableRowCollection()
                {
                }

                IEnumerator IEnumerable.GetEnumerator()
                {
                    return null;
                }
            }

            /// <summary>
            /// This class provides a wrapper for DataTables to allow for querying via LINQ.
            /// </summary>
            public class EnumerableRowCollection<TRow> : EnumerableRowCollection, IEnumerable<TRow>
            {
                private readonly DataTable _table;
                private readonly IEnumerable<TRow> _enumerableRows;
                private readonly List<Func<TRow, bool>> _listOfPredicates;

                // Stores list of sort expression in the order provided by user. E.g. order by, thenby, thenby descending..
                private readonly SortExpressionBuilder<TRow> _sortExpression;

                private readonly Func<TRow, TRow> _selector;

                #region Properties

                internal override Type ElementType
                {
                    get
                    {
                        return typeof(TRow);
                    }

                }

                internal IEnumerable<TRow> EnumerableRows
                {
                    get
                    {
                        return _enumerableRows;
                    }
                }

                internal override DataTable Table
                {
                    get
                    {
                        return _table;
                    }
                }


                #endregion Properties

                #region Constructors

                /// <summary>
                /// This constructor is used when Select operator is called with output Type other than input row Type.
                /// Basically fail on GetLDV(), but other LINQ operators must work.
                /// </summary>
                internal EnumerableRowCollection(IEnumerable<TRow> enumerableRows, bool isDataViewable, DataTable table)
                {
                    Debug.Assert(!isDataViewable || table != null, "isDataViewable bug table is null");

                    _enumerableRows = enumerableRows;
                    if (isDataViewable)
                    {
                        _table = table;
                    }
                    _listOfPredicates = new List<Func<TRow, bool>>();
                    _sortExpression = new SortExpressionBuilder<TRow>();
                }

                /// <summary>
                /// Basic Constructor
                /// </summary>
                internal EnumerableRowCollection(DataTable table)
                {
                    _table = table;
                    _enumerableRows = table.Rows.Cast<TRow>();
                    _listOfPredicates = new List<Func<TRow, bool>>();
                    _sortExpression = new SortExpressionBuilder<TRow>();
                }

                /// <summary>
                /// Copy Constructor that sets the input IEnumerable as enumerableRows
                /// Used to maintain IEnumerable that has linq operators executed in the same order as the user
                /// </summary>
                internal EnumerableRowCollection(EnumerableRowCollection<TRow> source, IEnumerable<TRow> enumerableRows, Func<TRow, TRow> selector)
                {
                    Debug.Assert(null != enumerableRows, "null enumerableRows");

                    _enumerableRows = enumerableRows;
                    _selector = selector;
                    if (null != source)
                    {
                        if (null == source._selector)
                        {
                            _table = source._table;
                        }
                        _listOfPredicates = new List<Func<TRow, bool>>(source._listOfPredicates);
                        _sortExpression = source._sortExpression.Clone(); //deep copy the List
                    }
                    else
                    {
                        _listOfPredicates = new List<Func<TRow, bool>>();
                        _sortExpression = new SortExpressionBuilder<TRow>();
                    }
                }

                #endregion Constructors

                #region PublicInterface
                IEnumerator IEnumerable.GetEnumerator()
                {
                    return GetEnumerator();
                }

                /// <summary>
                ///  This method returns an strongly typed iterator
                ///  for the underlying DataRow collection.
                /// </summary>
                /// <returns>
                ///   A strongly typed iterator.
                /// </returns>
                public IEnumerator<TRow> GetEnumerator()
                {
                    return _enumerableRows.GetEnumerator();
                }
                #endregion PublicInterface

                #region Add Single Filter/Sort Expression

                /// <summary>
                /// Used to add a filter predicate.
                /// A conjunction of all predicates are evaluated in LinqDataView
                /// </summary>
                internal void AddPredicate(Func<TRow, bool> pred)
                {
                    Debug.Assert(pred != null);
                    _listOfPredicates.Add(pred);
                }

                /// <summary>
                /// Adds a sort expression when Keyselector is provided but not Comparer
                /// </summary>
                internal void AddSortExpression<TKey>(Func<TRow, TKey> keySelector, bool isDescending, bool isOrderBy)
                {
                    AddSortExpression<TKey>(keySelector, Comparer<TKey>.Default, isDescending, isOrderBy);
                }

                /// <summary>
                /// Adds a sort expression when Keyselector and Comparer are provided.
                /// </summary>
                internal void AddSortExpression<TKey>(
                                    Func<TRow, TKey> keySelector,
                                    IComparer<TKey> comparer,
                                    bool isDescending,
                                    bool isOrderBy)
                {
                    DataSetUtil.CheckArgumentNull(keySelector, "keySelector");
                    DataSetUtil.CheckArgumentNull(comparer, "comparer");

                    _sortExpression.Add(
                            delegate (TRow input)
                            {
                                return (object)keySelector(input);
                            },
                            delegate (object val1, object val2)
                            {
                                return (isDescending ? -1 : 1) * comparer.Compare((TKey)val1, (TKey)val2);
                            },
                              isOrderBy);
                }

                #endregion Add Single Filter/Sort Expression

            }


            #endregion

            #region SortExpressionBuilder.cs

            /// <summary>
            /// This class represents a combined sort expression build using mutiple sort expressions.
            /// </summary>
            /// <typeparam name="T"></typeparam>
            internal class SortExpressionBuilder<T> : IComparer<List<object>>
            {
                /**
                 *  This class ensures multiple orderby/thenbys are handled correctly. Its semantics is as follows:
                 *  
                 * Query 1:
                 * orderby a
                 * thenby  b
                 * orderby c
                 * orderby d
                 * thenby  e
                 * 
                 * is equivalent to:
                 * 
                 * Query 2:
                 * orderby d
                 * thenby  e
                 * thenby  c
                 * thenby  a
                 * thenby  b
                 * 
                 **/

                //Selectors and comparers are mapped using the index in the list.
                //E.g: _comparers[i] is used with _selectors[i]

                LinkedList<Func<T, object>> _selectors = new LinkedList<Func<T, object>>();
                LinkedList<Comparison<object>> _comparers = new LinkedList<Comparison<object>>();

                LinkedListNode<Func<T, object>> _currentSelector = null;
                LinkedListNode<Comparison<object>> _currentComparer = null;


                /// <summary>
                /// Adds a sorting selector/comparer in the correct order
                /// </summary>
                internal void Add(Func<T, object> keySelector, Comparison<object> compare, bool isOrderBy)
                {
                    Debug.Assert(keySelector != null);
                    Debug.Assert(compare != null);
                    //Inputs are assumed to be valid. The burden for ensuring it is on the caller.

                    if (isOrderBy)
                    {
                        _currentSelector = _selectors.AddFirst(keySelector);
                        _currentComparer = _comparers.AddFirst(compare);
                    }
                    else
                    {
                        //ThenBy can only be called after OrderBy
                        Debug.Assert(_currentSelector != null);
                        Debug.Assert(_currentComparer != null);

                        _currentSelector = _selectors.AddAfter(_currentSelector, keySelector);
                        _currentComparer = _comparers.AddAfter(_currentComparer, compare);
                    }
                }



                /// <summary>
                /// Represents a Combined selector of all selectors added thusfar.
                /// </summary>
                /// <returns>List of 'objects returned by each selector'. This list is the combined-selector</returns>
                public List<object> Select(T row)
                {
                    List<object> result = new List<object>();

                    foreach (Func<T, object> selector in _selectors)
                    {
                        result.Add(selector(row));
                    }

                    return result;
                }



                /// <summary>
                /// Represents a Comparer (of IComparer) that compares two combined-selectors using
                /// provided comparers for each individual selector.
                /// Note: Comparison is done in the order it was Added.
                /// </summary>
                /// <returns>Comparison result of the combined Sort comparer expression</returns>
                public int Compare(List<object> a, List<object> b)
                {
                    Debug.Assert(a.Count == Count);

                    int i = 0;
                    foreach (Comparison<object> compare in _comparers)
                    {
                        int result = compare(a[i], b[i]);

                        if (result != 0)
                        {
                            return result;
                        }
                        i++;
                    }

                    return 0;
                }

                internal int Count
                {
                    get
                    {
                        Debug.Assert(_selectors.Count == _comparers.Count); //weak now that we have two dimensions
                        return _selectors.Count;
                    }
                }

                /// <summary>
                /// Clones the SortexpressionBuilder and returns a new object 
                /// that points to same comparer and selectors (in the same order).
                /// </summary>
                /// <returns></returns>
                internal SortExpressionBuilder<T> Clone()
                {
                    SortExpressionBuilder<T> builder = new SortExpressionBuilder<T>();

                    foreach (Func<T, object> selector in _selectors)
                    {
                        if (selector == _currentSelector.Value)
                        {
                            builder._currentSelector = builder._selectors.AddLast(selector);
                        }
                        else
                        {
                            builder._selectors.AddLast(selector);
                        }
                    }


                    foreach (Comparison<object> comparer in _comparers)
                    {
                        if (comparer == _currentComparer.Value)
                        {
                            builder._currentComparer = builder._comparers.AddLast(comparer);
                        }
                        else
                        {
                            builder._comparers.AddLast(comparer);
                        }
                    }

                    return builder;
                }

                /// <summary>
                /// Clones the SortExpressinBuilder and casts to type TResult.
                /// </summary>
                internal SortExpressionBuilder<TResult> CloneCast<TResult>()
                {
                    SortExpressionBuilder<TResult> builder = new SortExpressionBuilder<TResult>();

                    foreach (Func<T, object> selector in _selectors)
                    {
                        if (selector == _currentSelector.Value)
                        {
                            builder._currentSelector = builder._selectors.AddLast(r => selector((T)(object)r));
                        }
                        else
                        {
                            builder._selectors.AddLast(r => selector((T)(object)r));
                        }
                    }


                    foreach (Comparison<object> comparer in _comparers)
                    {
                        if (comparer == _currentComparer.Value)
                        {
                            builder._currentComparer = builder._comparers.AddLast(comparer);
                        }
                        else
                        {
                            builder._comparers.AddLast(comparer);
                        }
                    }

                    return builder;
                }

            } //end SortExpressionBuilder<T>

            #endregion

            #region DataSetUtil.cs

            internal static class DataSetUtil
            {
                #region CheckArgument
                internal static void CheckArgumentNull<T>(T argumentValue, string argumentName) where T : class
                {
                    if (null == argumentValue)
                    {
                        throw ArgumentNull(argumentName);
                    }
                }
                #endregion

                #region Trace
                private static T TraceException<T>(string trace, T e)
                {
                    Debug.Assert(null != e, "TraceException: null Exception");
                    if (null != e)
                    {
                        //Bid.Trace(trace, e.ToString()); // will include callstack if permission is available
                    }
                    return e;
                }

                private static T TraceExceptionAsReturnValue<T>(T e)
                {
                    return TraceException("<comm.ADP.TraceException|ERR|THROW> '%ls'\n", e);
                }
                #endregion

                #region new Exception
                internal static ArgumentException Argument(string message)
                {
                    return TraceExceptionAsReturnValue(new ArgumentException(message));
                }

                internal static ArgumentNullException ArgumentNull(string message)
                {
                    return TraceExceptionAsReturnValue(new ArgumentNullException(message));
                }

                internal static ArgumentOutOfRangeException ArgumentOutOfRange(string message, string parameterName)
                {
                    return TraceExceptionAsReturnValue(new ArgumentOutOfRangeException(parameterName, message));
                }

                internal static InvalidCastException InvalidCast(string message)
                {
                    return TraceExceptionAsReturnValue(new InvalidCastException(message));
                }

                internal static InvalidOperationException InvalidOperation(string message)
                {
                    return TraceExceptionAsReturnValue(new InvalidOperationException(message));
                }

                internal static NotSupportedException NotSupported(string message)
                {
                    return TraceExceptionAsReturnValue(new NotSupportedException(message));
                }
                #endregion

                #region new EnumerationValueNotValid
                static internal ArgumentOutOfRangeException InvalidEnumerationValue(Type type, int value)
                {
                    return ArgumentOutOfRange(string.Format("The {0} enumeration value, {1}, is not valid.", type.Name, value.ToString(System.Globalization.CultureInfo.InvariantCulture)), type.Name);
                }

                static internal ArgumentOutOfRangeException InvalidDataRowState(DataRowState value)
                {
#if DEBUG
                    switch (value)
                    {
                        case DataRowState.Detached:
                        case DataRowState.Unchanged:
                        case DataRowState.Added:
                        case DataRowState.Deleted:
                        case DataRowState.Modified:
                            Debug.Assert(false, "valid DataRowState " + value.ToString());
                            break;
                    }
#endif
                    return InvalidEnumerationValue(typeof(DataRowState), (int)value);
                }

                static internal ArgumentOutOfRangeException InvalidLoadOption(LoadOption value)
                {
#if DEBUG
                    switch (value)
                    {
                        case LoadOption.OverwriteChanges:
                        case LoadOption.PreserveChanges:
                        case LoadOption.Upsert:
                            Debug.Assert(false, "valid LoadOption " + value.ToString());
                            break;
                    }
#endif
                    return InvalidEnumerationValue(typeof(LoadOption), (int)value);
                }
                #endregion

                // only StackOverflowException & ThreadAbortException are sealed classes
                static private readonly Type StackOverflowType = typeof(System.StackOverflowException);
                static private readonly Type OutOfMemoryType = typeof(System.OutOfMemoryException);
                static private readonly Type ThreadAbortType = typeof(System.Threading.ThreadAbortException);
                static private readonly Type NullReferenceType = typeof(System.NullReferenceException);
                static private readonly Type AccessViolationType = typeof(System.AccessViolationException);
                static private readonly Type SecurityType = typeof(System.Security.SecurityException);

                static internal bool IsCatchableExceptionType(Exception e)
                {
                    // a 'catchable' exception is defined by what it is not.
                    Type type = e.GetType();

                    return ((type != StackOverflowType) &&
                             (type != OutOfMemoryType) &&
                             (type != ThreadAbortType) &&
                             (type != NullReferenceType) &&
                             (type != AccessViolationType) &&
                             !SecurityType.IsAssignableFrom(type));
                }
            }

            #endregion
        }
    }
}
