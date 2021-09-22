import json
from msfbe.webmodel import BaseHandler, service_handler, SimpleResults
import requests
import psycopg2
import types
import numpy as np
import math


class ParamType:
    STRING = 0
    BOOLEAN  = 1
    DATETIME = 2
    INTEGER = 3
    DECIMAL = 4

def param(
    name,
    type=ParamType.STRING,
    is_list=False,
    default_value=None,
    add_wildcard=False
):
    return {
        "name": name,
        "type": type,
        "is_list": is_list,
        "default_value": default_value,
        "add_wildcard": add_wildcard
    }


def column(
    name,
    index
):
    assert isinstance(name, types.StringType)
    assert isinstance(index, types.IntType)
    return {
        "name": name,
        "index": index
    }



class SummaryTypes:
    AVERAGE = 0
    SUM = 1
    MIN = 2
    MAX = 3
    COUNT = 4
    COUNT_DISTINCT = 5


def summary(
    name,
    type
):
    assert isinstance(name, types.StringType)
    assert isinstance(type, types.IntType)
    return {
        "name": name,
        "type": type
    }


class Summarizer:

    def __init__(self, field):
        self.field = field

    def next(self, row):
        raise Exception("Not implemented")

    def value(self):
        raise Exception("Not implemented")


class AverageSummarizer(Summarizer):

    def __init__(self, field):
        Summarizer.__init__(self, field)
        self.__values = []

    def next(self, row):
        row_value = row[self.field]
        if row_value is not None:
            self.__values.append(row_value)

    def value(self):
        v = np.mean(self.__values)
        return 0.0 if np.isnan(v) or math.isnan(v) else v

class SumSummarizer(Summarizer):

    def __init__(self, field):
        Summarizer.__init__(self, field)
        self.__value = 0.0

    def next(self, row):
        row_value = row[self.field]
        if row_value is not None:
            self.__value += row_value

    def value(self):
        return self.__value

class MinSummarizer(Summarizer):

    def __init__(self, field):
        Summarizer.__init__(self, field)
        self.__value = 9999999999

    def next(self, row):
        row_value = row[self.field]
        if row_value is not None:
            self.__value = np.min((self.__value, row_value))

    def value(self):
        return self.__value

class MaxSummarizer(Summarizer):

    def __init__(self, field):
        Summarizer.__init__(self, field)
        self.__value = -9999999999

    def next(self, row):
        row_value = row[self.field]
        if row_value is not None:
            self.__value = np.max((self.__value, row_value))

    def value(self):
        return self.__value


class CountSummarizer(Summarizer):

    def __init__(self, field):
        Summarizer.__init__(self, field)
        self.__count = 0

    def next(self, row):
        row_value = row[self.field]
        if row_value is not None:
            self.__count += 1

    def value(self):
        return self.__count


class CountDistinctSummarizer(Summarizer):

    def __init__(self, field):
        Summarizer.__init__(self, field)
        self.__value_list = []

    def next(self, row):
        row_value = row[self.field]
        if row_value is not None and not row_value in self.__value_list:
            self.__value_list.append(row_value)

    def value(self):
        return len(self.__value_list)



class QueryResultsSummarizer:

    def __init__(self, summary_spec):
        self.__summary_spec = summary_spec

        self.__summarizers = []
        for summary in summary_spec:
            summarizer = self.__create_summarizer(summary)
            self.__summarizers.append(summarizer)


    def __create_summarizer(self, summary):
        if summary["type"] == SummaryTypes.AVERAGE:
            return AverageSummarizer(summary["name"])
        elif summary["type"] == SummaryTypes.SUM:
            return SumSummarizer(summary["name"])
        elif summary["type"] == SummaryTypes.COUNT_DISTINCT:
            return CountDistinctSummarizer(summary["name"])
        elif summary["type"] == SummaryTypes.COUNT:
            return CountSummarizer(summary["name"])
        elif summary["type"] == SummaryTypes.MAX:
            return MaxSummarizer(summary["name"])
        elif summary["type"] == SummaryTypes.MIN:
            return MinSummarizer(summary["name"])
        else:
            raise Exception("Invalid summarizer type specified: %s" % summary.type)


    def __summarize_row(self, row):
        for summarizer in self.__summarizers:
            summarizer.next(row)

    def summarize_resultset(self, results):
        for row in results:
            self.__summarize_row(row)

        new_results = {}
        for summarizer in self.__summarizers:
            new_results[summarizer.field] = summarizer.value()
        return [new_results]





def _build_query_handler_class(_uri, _name, _sql, _params, _columns, _filters, _summarize = None):
    @service_handler
    class GenericQueryBasedHandler(BaseHandler):
        name = _name
        path = _uri
        description = ""
        params = {}
        singleton = True

        def __init__(self):
            BaseHandler.__init__(self)
            self.params = _params
            self.columns = _columns
            self.filters = _filters
            self.summarize_spec = _summarize
            self.sql = _sql


        def create_results_summarizer(self):
            if self.summarize_spec is not None and type(self.summarize_spec) == list and len(self.summarize_spec) > 0:
                return QueryResultsSummarizer(self.summarize_spec)
            else:
                return None

        def __query(self, config, params):
            conn = psycopg2.connect(dbname=config.get("database", "db.database"),
                                    user=config.get("database", "db.username"),
                                    password=config.get("database", "db.password"),
                                    host=config.get("database", "db.endpoint"),
                                    port=config.get("database", "db.port"))
            cur = conn.cursor()

            cur.execute(self.sql, params)

            results = cur.fetchall()
            cur.close()
            conn.close()

            return results


        def __format_result_row(self, row):
            result = {}
            for col in self.columns:
                result[col["name"]] = row[col["index"]]
            return result

        def __format_results(self, rows, params):
            results = []
            for row in rows:
                row_result = self.__format_result_row(row)
                for filter in self.filters:
                    row_result = filter(row_result, params)
                if row_result is not None:
                    results.append(row_result)
            return results

        def handle(self, computeOptions, **args):
            param_map = {}
            for param in self.params:
                if param["type"] == ParamType.STRING:
                    param_value = computeOptions.get_argument(param["name"], param["default_value"])
                    if param["add_wildcard"] is True:
                        if param_value is not None and len(param_value) == 0:
                            param_value = "%"
                        else:
                            if param_value is not None and param_value[0] != '%':
                                param_value = "%%%s"%param_value
                            if param_value is not None and param_value[-1] != '%':
                                param_value = "%s%%"%param_value
                    param_map[param["name"]] = param_value
                elif param["type"] == ParamType.BOOLEAN:
                    param_map[param["name"]] = computeOptions.get_boolean_arg(param["name"], param["default_value"])
                elif param["type"] == ParamType.DATETIME:
                    param_map[param["name"]] = computeOptions.get_datetime_arg(param["name"], param["default_value"])
                elif param["type"] == ParamType.DECIMAL:
                    param_map[param["name"]] = computeOptions.get_decimal_arg(param["name"], param["default_value"])
                elif param["type"] == ParamType.INTEGER:
                    param_map[param["name"]] = computeOptions.get_int_arg(param["name"], param["default_value"])
                else:
                    raise Exception("Unsupported or invalid parameter type")

            rows = self.__query(args["webconfig"], param_map)
            results = self.__format_results(rows, param_map)

            summarize = self.create_results_summarizer()
            if summarize is not None:
                results = summarize.summarize_resultset(results)

            return SimpleResults(results)


def create_query_based_handler(
    uri,
    name,
    sql,
    params=[],
    columns=[],
    filters=[],
    summarize=[]
):
    _build_query_handler_class(uri, name, sql, params, columns, filters, summarize)


