from multiprocessing import Process
from nlde.operators.xproject import Xproject
from nlde.operators.xdistinct import Xdistinct

class TreePlan(object):
    """
    Represents a plan to be executed by the engine.

    It is composed by a left node, a right node, and an operators node.
    The left and right nodes can be leaves to contact sources, or subtrees.
    The operators node is a physical operators, provided by the engine.

    The execute() method evaluates the plan.
    It creates a process for every node of the plan.
    The left node is always evaluated.
    If the right node is an independent operators or a subtree, it is evaluated.
    """

    def __init__(self, operator, variables, join_vars, sources, left, right, height=0, res=0):
        self.operator = operator
        self.vars =  set([str(val) for val in variables])
        self.join_vars = join_vars #set([str(val) for val in join_vars])
        sources_dct =  {int(key): set([str(val) for val in value]) for key, value in sources.items()}
        self.sources = sources_dct
        self.left = left
        self.right = right
        self.height = height
        self.total_res = res
        self.p = None
        self.cost = None
        self.cardinality = None



    def to_dict(self):
        return {
            "type" : "TreePlan",
            "operator" : self.operator.to_dict(),
            "variables" : list(self.vars),
            "join_vars" : list(self.join_vars),
            "sources" : { key : list(val) for key, val in self.sources.items()},
            "left" : self.left.to_dict(),
            "right": self.right.to_dict(),
            "height" : self.height,
            "res" : self.total_res,
            "cardinality" : self.cardinality,
            "selectivity" : self.selectivity,
            "cost" : self.cost
        }

    def __str__(self):
        return "({} {})".format(self.left, self.right)
        #return "({}, {}, ({}, {}))".format(self.left, self.right, self.cardinality, self.cost)

    @property
    def is_triple_pattern(self):
        return False

    def aux(self, n):
        # Node string representation.
        s = n + str(self.operator) + "\n" + n + str(self.vars) + "\n"

        # Left tree plan string representation.
        if self.left:
            s = s + str(self.left.aux(n + "  "))

        # Right tree plan string representation.
        if self.right:
            s = s + str(self.right.aux(n + "  "))

        return s

    @property
    def variables(self):
        return self.vars

    @property
    def join_variables(self):
        return self.join_vars

    @property
    def query(self):
        if self.right:
            return "{} {}".format(self.left.query, self.right.query)
        else:
            return self.left.query

    @property
    def variables_dict(self):
        v_dict = {
            "s" : set(),
            "p" : set(),
            "o" : set(),
        }
        for key, value in self.right.variables_dict.items():
            if value:
                v_dict[key].update(value)
        for key, value in self.left.variables_dict.items():
            if value:
                v_dict[key].update(value)
        return v_dict

    def compute_cardinality(self, card_est_model):
        if self.right:
            # If Join Operator
            self.cardinality = max(card_est_model.join_cardinality(self.left, self.right), 1.0)
        else:
            # If Project Operator
            self.cardinality =  self.left.compute_cardinality(card_est_model)

        return self.cardinality

    @property
    def selectivity(self):
        return min(self.right.selectivity, self.left.selectivity)


    def compute_cost(self, cost_model):
        treeplan_cost = cost_model[type(self.operator)]

        if isinstance(self.operator, Xproject) or isinstance(self.operator, Xdistinct):
            self.cardinality = self.left.cardinality
        else:
            if cost_model.switch and self in cost_model.switch: #and self.join_type >= 2:
                self.cardinality = cost_model.cardinality_estimation.join_cardinality(self.left, self.right, func=cost_model.switch_function)
            else:
                self.cardinality = cost_model.cardinality_estimation.join_cardinality(self.left, self.right)

        cost_self = treeplan_cost(self.left, self.right)


        if isinstance(self.left, TreePlan):
            cost_self += self.left.cost
        if isinstance(self.right, TreePlan):
            cost_self += self.right.cost


        self.cost = cost_self
        return self.cost

    @property
    def join_type(self):

        e1_vars = self.left.variables_dict
        e2_vars = self.right.variables_dict
        if len(set(e1_vars['s']).intersection(set(e2_vars['s']))) > 0:
            return 1
        elif len(set(e1_vars['s']).intersection(set(e2_vars['o']))) > 0:
            return 2
        elif len(set(e1_vars['o']).intersection(set(e2_vars['s']))) > 0:
            return 2
        elif len(set(e1_vars['o']).intersection(set(e2_vars['o']))) > 0:
            return 3
        return -1

    def execute(self, operators_input_queues,  eddies_queues, p_list, operators_desc):

        operator_inputs = operators_input_queues[self.operator.id_operator]
        # Execute left sub-plan.
        if self.left:
            # Case: Plan leaf (asynchronous).
            if self.left.__class__.__name__ == "IndependentOperator":
                q = operators_desc[self.operator.id_operator][self.left.sources.keys()[0]]
                p1 = Process(target=self.left.execute,
                             args=(operators_input_queues[self.operator.id_operator][q], None, eddies_queues, p_list,))
                p1.start()
                p_list.put(p1.pid)

            # Case: Tree plan.
            elif self.left.__class__.__name__ == "TreePlan":
                p1 = Process(target=self.left.execute,
                             args=(operators_input_queues, eddies_queues, p_list, operators_desc,))

                p1.start()
                p_list.put(p1.pid)

            # Case: Array of independent operators.
            else:
                for elem in self.left:
                    q = operators_desc[self.operator.id_operator][elem.sources.keys()[0]]
                    p1 = Process(target=elem.execute,
                                 args=(operators_input_queues[self.operator.id_operator][q], None, eddies_queues, p_list,))

                    p1.start()
                    p_list.put(p1.pid)

        # Execute right sub-plan.
        if self.right and ((self.right.__class__.__name__ == "TreePlan") or (self.right.__class__.__name__ == "IndependentOperator")):

            # Case: Plan leaf (asynchronous).
            if self.right.__class__.__name__ == "IndependentOperator":
                q = operators_desc[self.operator.id_operator][self.right.sources.keys()[0]]
                p2 = Process(target=self.right.execute,
                             args=(None, operators_input_queues[self.operator.id_operator][q], eddies_queues, p_list,))
            # Case: Tree plan.
            else:
                p2 = Process(target=self.right.execute,
                             args=(operators_input_queues, eddies_queues, p_list, operators_desc,))

            p2.start()
            p_list.put(p2.pid)
            #right = operators_input_queues[self.operator.id_operator]

        # Right sub-plan. Case: Plan leaf (dependent).
        else:
        # TODO: Change this. Uncomment line below
        #elif self.right:
            operator_inputs = operator_inputs + [self.right]
            #right = self.right

        # Create a process for the operator node.
        self.p = Process(target=self.operator.execute,
                         args=(operator_inputs, eddies_queues,))

        self.p.start()
        p_list.put(self.p.pid)