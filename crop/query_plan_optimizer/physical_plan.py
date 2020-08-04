from nlde.operators.independent_operator import IndependentOperator
from nlde.operators.dependent_operator import DependentOperator
from nlde.planner.plan import Plan
from nlde.engine.contactsource import get_metadata_ldf
from nlde.planner.tree_plan import TreePlan
from nlde.util.querystructures import TriplePattern
from nlde.operators.fjoin import Fjoin
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.xproject import Xproject
from nlde.operators.xdistinct import Xdistinct
import numpy as np
from itertools import combinations, chain

class PhysicalPlan(object):

    def __init__(self, source, eddies, plan, query):
        self.source = source

        self.eddies = eddies
        self.physical_plan = None
        self.id_operator = 0
        self.sources_desc = {}
        self.eofs_desc = {}
        self.dependent_sources = 0
        self.independent_source = 0
        self.operators_vars = {}
        self.operators = []
        self.operators_sym = {}
        self.sources = {}
        self.query = query

        # Maps each operator to the sources that pass through it
        # Also indicating if it is the left (0) or right (1) input
        self.operators_desc = {}
        self.plan_order = {}
        self.source_id = 0

        # Maps source to an integer representing the operators it passes through
        self.source_by_operator = {}

        # Store nodes in plan
        self.nodes = set()

        self.logical_plan = plan
        tree = self.create_subplan_by_join(plan[0], plan[1], plan[2])

        self.sources_desc = self.source_by_operator

        # Adds the projection operator to the plan.
        if self.query.projection:
            op = Xproject(self.id_operator, query.projection, eddies)
            self.operators.append(op)
            tree = TreePlan(op,
                            tree.vars, tree.join_vars, tree.sources, tree, None, tree.height + 1, tree.total_res)

            # Update signature of tuples.
            self.operators_sym.update({self.id_operator: False})
            self.operators_desc[self.id_operator] = {}
            for source in tree.sources:
                self.operators_desc[self.id_operator].update({source: 0})
                self.eofs_desc[source] = self.eofs_desc[source] | pow(2, self.id_operator)
                self.sources_desc[source] = self.sources_desc[source] | pow(2, self.id_operator)
            self.plan_order[self.id_operator] = tree.height
            self.operators_vars[self.id_operator] = tree.vars
            self.id_operator += 1


        # Adds the distinct operator to the plan.
        if query.distinct:
            op = Xdistinct(self.id_operator, eddies)
            self.operators.append(op)
            tree = TreePlan(op, tree.vars, tree.join_vars, tree.sources,
                            tree, None, tree.height + 1, tree.total_res)

            # Update signature of tuples.
            self.operators_sym.update({self.id_operator: False})
            self.operators_desc[self.id_operator] = {}
            for source in tree.sources:
                self.operators_desc[self.id_operator].update({source: 0})
                self.eofs_desc[source] = self.eofs_desc[source] | pow(2, self.id_operator)
                self.sources_desc[source] = self.sources_desc[source] | pow(2, self.id_operator)
            self.plan_order[self.id_operator] = tree.height
            self.operators_vars[self.id_operator] = tree.vars
            self.id_operator += 1

        self.physical_plan = Plan(query_tree=tree, tree_height=tree.height,
                                  operators_desc=self.operators_desc, sources_desc=self.source_by_operator,
                                  plan_order=self.plan_order, operators_vars=self.operators_vars,
                                  independent_sources=self.independent_source,
                                  operators_sym=self.operators_sym, operators=self.operators)

        self.independent_sources = self.physical_plan.independent_sources

        self.__average_cost = None
        self.__cardinality = {}


    def __str__(self):
        return str(self.physical_plan.tree)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __len__(self):
        # Number of sources (i.e., triple patterns)
        return self.independent_sources + self.dependent_sources

    def cost(self, cost_model):
        return self.physical_plan.tree.compute_cost(cost_model)

    def average_cost(self, cost_model):

        base_cost = self.cost(cost_model)
        nodes_cost = [base_cost]
        s = self.nodes
        s = [n for n in self.nodes if n.join_type >= 2]
        node_subsets = chain.from_iterable(combinations(s, r) for r in range(1, len(s) + 1))
        for nodes in node_subsets:
            if len(nodes) > 0:
                cost_model.set_switch(nodes, lambda x,y: sum([x,y]))
                nodes_cost.append(self.physical_plan.tree.compute_cost(cost_model))
                cost_model.set_switch(nodes, lambda x,y: max(x, y))
                nodes_cost.append(self.physical_plan.tree.compute_cost(cost_model))
                cost_model.set_switch(nodes, lambda x,y: max(x / y, y / x))
                nodes_cost.append(self.physical_plan.tree.compute_cost(cost_model))

                cost_model.set_switch(None, None)
                cost_model.set_function(None)

        self.__average_cost = np.median(nodes_cost)
        # Taking the max does seem to provide better results
        self.physical_plan.robustness_val = self.__average_cost
        return self.__average_cost

    def cardinality(self, cost_model):
        return self.physical_plan.tree.compute_cardinality(cost_model.cardinality_estimation)

    @property
    def tree(self):
        return self.physical_plan.tree

    @property
    def is_bushy(self):
        return self.physical_plan.is_bushy

    @property
    def height(self):
        return self.maxDepth(self.tree)

    def maxDepth(self, node):
        if isinstance(node, IndependentOperator) or isinstance(node, DependentOperator) :
            return 0

        else:

            # Compute the depth of each subtree
            lDepth = self.maxDepth(node.left)
            rDepth = self.maxDepth(node.right)

            # Use the larger one
            if (lDepth > rDepth):
                return lDepth + 1
            else:
                return rDepth + 1

    def join_subplans(self, left, right, join_type=None):

        # Get Metadata for join
        if isinstance(left, TriplePattern):
            # Get cardinality; Query only if necessary
            left_card = left.count if left.count else get_metadata_ldf(self.source, left)
        else:
            left_card = left.total_res

        if isinstance(right, TriplePattern):
            # Get cardinality; Query only if necessary
            right_card = right.count if right.count else get_metadata_ldf(self.source, right)
        else:
            right_card = right.total_res

        # Pre-decided Join Type
        if join_type:
            xn_join = True if issubclass(join_type, Xnjoin) else False

            if xn_join:
                # Switch sides for NLJ
                if left_card > right_card:
                    tmp = left
                    left = right
                    right = tmp

        # Decide based in heursitics
        else:
            # Decide Join Type: xn = NLJ, FJ = SHJ
            if isinstance(left, IndependentOperator):
                xn_join = True if left_card < (right_card / 100.0) else False
            else:
                xn_join = True if left_card <= right_card else False



        # Tree Plans as Leafs
        if isinstance(left, TreePlan):
            leaf_left = left
            for source in left.sources.keys():
                self.source_by_operator[source] = self.source_by_operator[source] | pow(2, self.id_operator)
                self.operators_desc.setdefault(self.id_operator, {})[source] = 0

            self.operators_desc.setdefault(self.id_operator, {})[self.source_id] = 0

        if isinstance(right, TreePlan):
            leaf_right = right
            for source in right.sources.keys():
                self.source_by_operator[source] = self.source_by_operator[source] | pow(2, self.id_operator)
                self.operators_desc.setdefault(self.id_operator, {})[source] = 1

        if xn_join and isinstance(left, TreePlan) and isinstance(right, TreePlan):
            print("Invalid plan")

        # Operator Leafs
        if isinstance(left, TriplePattern):

            self.eofs_desc.update({self.source_id: 0})
            self.sources[self.source_id] = left.variables
            self.source_by_operator[self.source_id] = pow(2, self.id_operator)
            self.operators_desc.setdefault(self.id_operator, {})[self.source_id] = 0

            # Base on join, create operator
            # If SHJ(FJ), use IO
            # Or if it is a NLJ(XN) and left is a TP, then use IO
            if (not xn_join) or (xn_join and isinstance(right, TriplePattern)):
                leaf_left = IndependentOperator(self.source_id, self.source, left,
                                                self.source_by_operator, left.variables, self.eddies,
                                                self.source_by_operator)
                self.independent_source += 1

            elif xn_join and isinstance(right, TreePlan):
                leaf_left = DependentOperator(self.source_id, self.source, left,
                                              self.source_by_operator, left.variables, self.source_by_operator)
                self.dependent_sources += 1

            leaf_left.total_res = left_card
            self.source_id += 1

        if isinstance(right, TriplePattern):

            self.eofs_desc.update({self.source_id: 0})
            self.sources[self.source_id] = right.variables
            self.source_by_operator[self.source_id] = pow(2, self.id_operator)
            self.operators_desc.setdefault(self.id_operator, {})[self.source_id] = 1

            # Base on join, create operator
            if not xn_join:
                leaf_right = IndependentOperator(self.source_id, self.source, right,
                                                 self.source_by_operator, right.variables, self.eddies,
                                                 self.source_by_operator)
                self.independent_source += 1

            else:
                leaf_right = DependentOperator(self.source_id, self.source, right,
                                               self.source_by_operator, right.variables, self.source_by_operator)
                self.dependent_sources += 1


            leaf_right.total_res = right_card
            self.source_id += 1

        ## Create Joins
        join_vars = set(left.variables).intersection(right.variables)
        all_variables = set(left.variables).union(right.variables)

        self.operators_vars[self.id_operator] = join_vars

        res = leaf_left.total_res + leaf_right.total_res
        self.plan_order[self.id_operator] = max(leaf_left.height, leaf_right.height)

        # Place Join
        if xn_join:  # NLJ
            op = Xnjoin(self.id_operator, join_vars, self.eddies)
            self.operators_sym.update({self.id_operator: True})

            # If Right side has to be DP
            if not isinstance(leaf_right, DependentOperator):
                # Switch Leafs
                tmp = leaf_right
                leaf_right = leaf_left
                leaf_left = tmp

                # Update operators_descs for current operator id
                for key, value in self.operators_desc[self.id_operator].items():
                    # Leaf Right is now the DP and needs to be input Right, i.e. 1
                    if key == leaf_right.sources.keys()[0]:
                        self.operators_desc[self.id_operator][key] = 1
                    # All other will be on the left input
                    else:
                        self.operators_desc[self.id_operator][key] = 0

        else:  # SHJ
            op = Fjoin(self.id_operator, join_vars, self.eddies)
            self.operators_sym.update({self.id_operator: False})

        # Add Operator
        self.operators.append(op)

        tree_height = max(leaf_left.height, leaf_right.height) + 1
        #tree_sources = {k: v for k, v in self.sources.items()}
        # 2020-03-04: Changed here to route everything properly
        tree_sources = dict(leaf_left.sources)
        tree_sources.update(dict(leaf_right.sources))
        # Create Tree Plan
        tree_plan = TreePlan(op, all_variables, join_vars, tree_sources,
                             leaf_left, leaf_right, tree_height, res)

        if isinstance(op, Xnjoin) and isinstance(leaf_left, TreePlan) and isinstance(leaf_right, TreePlan):
            raise Exception
        self.nodes.add(tree_plan)

        self.id_operator += 1
        return tree_plan

    def create_subplan(self, left, right):

        if isinstance(left, TriplePattern) and isinstance(right, TriplePattern):
            return self.join_subplans(left, right)

        elif isinstance(left, TriplePattern):
            return self.join_subplans(left, self.create_subplan(right[0], right[1]))

        elif isinstance(right, TriplePattern):
            return self.join_subplans(self.create_subplan(left[0], left[1]), right)
        else:
            return self.join_subplans(self.create_subplan(left[0], left[1]), self.create_subplan(right[0], right[1]))


    def create_subplan_by_join(self, left, right, join_type):
        if isinstance(left, TriplePattern) and isinstance(right, TriplePattern):
            return self.join_subplans(left, right, join_type=join_type)

        elif isinstance(left, TriplePattern):
            return self.join_subplans(left, self.create_subplan_by_join(right[0], right[1], right[2]), join_type=join_type)

        elif isinstance(right, TriplePattern):
            return self.join_subplans(self.create_subplan_by_join(left[0], left[1], left[2]), right, join_type=join_type)
        else:
            return self.join_subplans(self.create_subplan_by_join(left[0], left[1], left[2]), self.create_subplan_by_join(right[0], right[1], right[2]), join_type=join_type)
