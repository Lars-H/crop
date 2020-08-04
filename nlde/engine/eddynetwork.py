"""
Created on Mar 23, 2015

@author: Maribel Acosta

Updated on Mar 10, 2020
@author: Anonymous

"""
from eddyoperator import EddyOperator
from nlde.util.sparqlparser import parse
from crop.query_plan_optimizer.idp_optimizer import IDP_Optimizer
from multiprocessing import Process, Queue
from time import time

class EddyNetwork(object):
    
    def __init__(self, **kwargs):

        self.query = kwargs.get("query", None)
        self.policy = kwargs.get("policy")
        self.source = kwargs.get("source", "")
        self.n_eddy = kwargs.get("n_eddy", 2)
        self.explain = kwargs.get("explain", False)
        self.optimization = kwargs.get("optimization", "CROP")
        self.eofs = 0

        self.independent_operators = []
        self.join_operators = []
        self.eddy_operators = []
        
        self.eddies_queues = []
        self.operators_input_queues = {}
        self.operators_left_queues = []
        self.operators_right_queues = []

        self.tree = None
        self.operators_desc = None
        self.sources_desc = None
        self.eofs_operators_desc = None

        # Inits for robustness experiments
        # Cost Model, Optimization Time, Cheapest and Most Robust Plans
        self.cost_model = kwargs.get("cost_model")
        self.robust_model = kwargs.get("robust_model")


        ## IDP Optimizer Setup
        k = kwargs.get("k")
        top_t = kwargs.get("top_t")
        adaptive_k = kwargs.get("adaptive_k")
        enable_robustplan = self.optimization == "CROP"
        robustness_threshold = kwargs.get("robust_threshold")
        cost_threshold = kwargs.get("cost_threshold")
        self.idp_optimizer = IDP_Optimizer(eddies=self.n_eddy, source=self.source, cost_model=self.cost_model,
                                            robust_model=self.robust_model, k=k, top_t=top_t, adaptive_k=adaptive_k,
                                            enable_robustplan=enable_robustplan,
                                            robustness_threshold=robustness_threshold, cost_threshold=cost_threshold)


        # Stats
        self.triple_pattern_cnt = -1

        self.p_list = Queue()
        # If a Query plan is provided
        self.queryplan = kwargs.get("queryplan", None)

        if self.queryplan is None and self.query is None:
            raise ValueError("Missing Argument, either Query String or Query Plan")

        self.plan = self.__get_query_plan()



    def __get_query_plan(self):

        plan = None
        if self.queryplan:
            return self.queryplan
        else:
            # Parse SPARQL query.
            queryparsed = parse(self.query)
            self.triple_pattern_cnt = len(queryparsed.where.left.triple_patterns)

            # Start Timer
            start = time()
            # Create Plan using CROP Approach
            plan = self.idp_optimizer.iterative_dynamic_programming1(queryparsed)
            # Time the execution
            self.optimization_time = time() - start

            return plan

    def execute_plan(self, outputqueue, plan):

        self.tree = plan.tree
        self.eofs_operators_desc = plan.operators_desc
        self.sources_desc = plan.sources_desc
        self.operators_desc = plan.operators_desc
        self.eofs = plan.independent_sources
        self.policy.initialize_priorities(plan.plan_order)

        # Create eddies queues.
        for i in range(0, self.n_eddy+1):
            self.eddies_queues.append(Queue())

        # Create operators queues (left and right).
        for op in plan.operators:
            self.operators_input_queues.update({op.id_operator: []})
            for i in range(0, op.independent_inputs):
                self.operators_input_queues[op.id_operator].append(Queue())

        for i in range(1, self.n_eddy+1):
            eddy = EddyOperator(i, self.policy, self.eddies_queues, self.operators_desc, self.operators_input_queues,
                                plan.operators_vars, outputqueue, plan.independent_sources, self.eofs_operators_desc,
                                plan.operators_sym, plan.operators)
            p = Process(target=eddy.execute)
            p.start()
            self.p_list.put(p.pid)

        self.tree.execute(self.operators_input_queues, self.eddies_queues, self.p_list, self.operators_desc)

    def execute(self, outputqueue):
        self.execute_plan(outputqueue, self.plan)
