from multiprocessing import Process, Queue
from nlde.engine.contactsource import contact_ldf_server
#from nlde.engine.contactsource_fast import contact_ldf_server
from nlde.operators.operatorstructures import Tuple


class IndependentOperator(object):
    """
    Implements a plan leaf that can be resolved asynchronously.

    The execute() method reads tuples from the input queue and
    place them in the output queue.
    """

    def __init__(self, sources, server, query, sources_desc, variables=None, eddies=2, eofs_desc={}, res=-1):
        if variables is None:
            variables = []

        self.server = server
        #self.q = Queue()
        #self.sources = {sources: set([str(var)  for var in variables])}
        self.sources = {sources: variables}
        self.server = server
        self.query = query
        #self.sources_desc = sources_desc
        self.sources_desc = sources_desc #{int(key): int(value) for key, value in sources_desc.items()}

        # False source desc
        self.f_sources_desc = {int(key): int(value) for key, value in sources_desc.items()}
        self.vars = set([str(var) for var in variables])
        self.join_vars = set([str(var) for var in variables])
        self.total_res = res
        self.height = 0
        self.eddies = eddies
        self.eofs_desc = eofs_desc
        self.p = None
        self.cost = None

    def to_dict(self):
        return {
                "type" : "IndependentOperator",
                "server" : self.server,
                "sources" : { key : list(val) for key, val in self.sources.items()},
                "query" : self.query.to_dict(),
                "sources_desc" : self.sources_desc,
                "variables" : list(self.vars),
                "res" : self.total_res,
                "eofs_desc" : self.eofs_desc,
                "eddies" : self.eddies,
                "cardinality": self.cardinality,
                "selectivity": self.selectivity,
                "cost" : self.cost
            }

    def __str__(self):
        return "Independent: {} ({})".format(self.query, self.cardinality)

    def aux(self, _):
        return " Independent: " + str(self.query)

    #def compute_cardinality(self, cost_model):
    #    return self.query.cardinality

    @property
    def variables_dict(self):
        return self.query.variables_dict

    @property
    def cardinality(self):
        return self.query.cardinality

    @property
    def selectivity(self):
        return self.query.selectivity

    def compute_cost(self, cost_model):
        cost_function = cost_model[type(self)]
        self.cost = cost_function(self)
        return self.cost

    def execute(self, left, right, outputqueues, p_list=None):
        self.q = Queue()
        # Determine the output queue.
        if not left:
            outq = right
        else:
            outq = left

        # Contact source.
        self.p = Process(target=contact_ldf_server, args=(self.server, self.query, self.q,))
        self.p.start()

        if p_list:
            p_list.put(self.p.pid)

        # Initialize signature of tuples.
        ready = self.sources_desc[self.sources.keys()[0]]

        #print(self.sources_desc, self.f_sources_desc)
        #print(self.sources_desc[self.sources.keys()[0]], self.f_sources_desc[self.sources.keys()[0]])

        done = 0
        sources = self.sources.keys()

        # Read answers from the source.
        data = self.q.get(True)
        count = 0
        while data != "EOF":
            count = count + 1
            # Create tuple and put it in the output queue.
            outq.put(Tuple(data, ready, done, sources))
            # Read next answer.
            data = self.q.get(True)

        # Close queue.
        ready_eof = self.eofs_desc[self.sources.keys()[0]]
        outq.put(Tuple("EOF", ready_eof, done, sources))
        outq.close()
        self.p.terminate()