from multiprocessing import Process, Queue
from nlde.engine.contactsource import contact_ldf_server, get_metadata_ldf
#from nlde.engine.contactsource_fast import contact_ldf_server, get_metadata_ldf
from nlde.operators.operatorstructures import Tuple
from nlde.util.querystructures import TriplePattern, Argument

class DependentOperator(object):
    """
    Implements a plan leaf that is resolved by a dependent physical operator.

    The execute() method reads tuples from the input queue and
    place them in the output queue.
    """

    def __init__(self, sources, server, query, sources_desc, variables=None, res=0):

        if variables is None:
            variables = []

        self.server = server
        #self.q = Queue()

        if isinstance(sources, int):
            self.sources =  {sources: set([str(var)  for var in variables])}
        else:
            self.sources = sources

        #self.sources_desc = {int(key): value for key, value in sources_desc.items()}
        self.sources_desc = sources_desc

        self.server = server
        self.query = query
        self.vars = set(variables)
        self.join_vars = set(variables)
        self.total_res = res
        self.height = 0
        self.p = None
        self.cost = None

    def to_dict(self):
        return {
            "type" : "DependentOperator",
            "server" : self.server,
            "sources" : { key : list(val) for key, val in self.sources.items()},
            "query" : self.query.to_dict(),
            "sources_desc" : self.sources_desc,
            "variables" : list(self.vars),
            "res" : self.total_res,
            "cardinality": self.cardinality,
            "selectivity": self.selectivity,
            "cost" : self.cost
        }


    def __str__(self):
        return "Dependent: {} ({})".format(self.query, self.cardinality)


    def aux(self, n):
        return "Dependent: ", self.query


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

    def execute(self, variables, instances, outputqueue, p_list=None):
        self.q = Queue()
        # Copy the query array and obtain variables.
        query = [self.query.subject.value, self.query.predicate.value, self.query.object.value]
        variables = list(variables)

        # Instantiate variables in the query.
        inst = {}
        for i in variables:
            inst.update({i: instances[i]})
            #inst_aux = str(instances[i]).replace(" ", "%%%")
            # Remove the %%% replacement as it does not work with the current LDF Server implementation
            inst_aux = str(instances[i])
            for j in (0, 1, 2):
                if query[j] == "?" + i:
                    query[j] = inst_aux


        tp = TriplePattern(Argument(query[0]), Argument(query[1]), Argument(query[2]))
        if False and get_metadata_ldf(self.server, tp) == 0:
            # Ready and done vectors.
            ready = self.sources_desc[self.sources.keys()[0]]
            done = 0
            sources = self.sources.keys()
            outputqueue.put(Tuple("EOF", ready, done, sources))

        else:

            # Create process to contact source.
            aux_queue = Queue()
            self.p = Process(target=contact_ldf_server, args=(self.server, tp, aux_queue,))
            self.p.start()
            sources = self.sources.keys()

            if p_list:
                p_list.put(self.p.pid)

            # Ready and done vectors.
            ready = self.sources_desc[self.sources.keys()[0]]
            done = 0

            # Get answers from the source.
            data = aux_queue.get(True)
            while data != "EOF":

                # TODO: Check why this is needed.
                data.update(inst)

                # Create tuple and put it in output queue.
                outputqueue.put(Tuple(data, ready, done, sources))

                # Get next answer.
                data = aux_queue.get(True)

            # Close the queue
            aux_queue.close()
            outputqueue.put(Tuple("EOF", ready, done, sources))
            self.p.terminate()