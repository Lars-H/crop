from nlde.util.querystructures import TriplePattern
from nlde.operators.xnjoin import Xnjoin

class LogicalPlan(object):

    def __init__(self, L, R=None, join=None):

        triple_patterns = set()
        self.__vars = L.variables
        if isinstance(L,TriplePattern):
            triple_patterns.add(L)
        else:
            triple_patterns = triple_patterns.union(L.triple_patterns)

        if R:
            self.__vars = self.__vars.union(R.variables)
            triple_patterns = triple_patterns.union(R.triple_patterns)

            # Place L and R for XNJoin correctly
            if join == Xnjoin:
                if isinstance(R, LogicalPlan) and L.is_triple_pattern:
                    tmp = L
                    L = R
                    R = tmp
            else:
                if L.cardinality > R.cardinality:
                    tmp = L
                    L = R
                    R = tmp

        self.__join = join
        self.__L = L
        self.__R = R

        self.__triple_patterns = frozenset(triple_patterns)
        self.cost = None

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __str__(self):
        return str((str(self.__L), str(self.__R), self.__join))

    def __len__(self):
        return len(self.triple_patterns)

    @property
    def L(self):
        return self.__L

    @property
    def R(self):
        return self.__R

    @property
    def height(self):
        if self.is_triple_pattern:
            return 0
        else:

            # Compute the depth of each subtree
            lDepth = self.__L.height
            rDepth = self.__R.height

            # Use the larger one
            if (lDepth > rDepth):
                return lDepth + 1
            else:
                return rDepth + 1

    @property
    def join(self):
        return self.__join

    @property
    def logical_plan(self):
        if not self.__R and not self.__join:
            return self.__L
        return (self.__L.logical_plan, self.__R.logical_plan, self.__join)

    @property
    def variables(self):
        return self.__vars

    @property
    def triple_patterns(self):
        return set(self.__triple_patterns)

    def __add__(self, other):
        return len(self.variables.intersection(other.variables))

    @property
    def is_triple_pattern(self):
        if not self.__R and not self.__join:
            return True
        return False

    @property
    def cardinality(self):
        if self.is_triple_pattern:
            return self.__L.cardinality
        else:
            return self.__cardinality

    @property
    def worst_case_cardinality(self):
        if self.is_triple_pattern:
            return self.__L.cardinality
        return self.__wc_cardinality

    def compute_cost(self, cost_model):

        if self.is_triple_pattern:
            cost = cost_model.access_operator(self)
            return cost
        else:
            plan_cost_function = cost_model[self.__join]
            self.__cardinality = cost_model.cardinality_estimation.join_cardinality(self.__L, self.__R)
            cost_self = plan_cost_function(self.__L, self.__R)

            if not self.L.is_triple_pattern:
                cost_self += self.L.cost
            if not self.R.is_triple_pattern:
                cost_self += self.R.cost

            self.cost = cost_self
            return self.cost


    def __getitem__(self, item):
        return (self.__L, self.__R, self.__join)[item]