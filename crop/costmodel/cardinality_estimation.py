class CardinalityEstimation(object):

    def __init__(self):
        self.__switch = None
        self.__func = lambda x, y: min(x,y)

    def set_function(self, func):
        self.__func = func

    def function(self, x, y):
        return self.__func(x, y)

    def __str__(self):
        return "CE"

    def join_cardinality(self, e1, e2, func=None):

        if not e1.cardinality:
            e1.compute_cardinality(self)
        if not e2.cardinality:
            e2.compute_cardinality(self)

        if func:
            return func(e1.cardinality, e2.cardinality)
        elif self.__func:
            return self.__func(e1.cardinality, e2.cardinality)
        else:
            return min(e1.cardinality, e2.cardinality)
