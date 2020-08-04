"""
Created on Mar 21, 2015

Physical operator that implements a JOIN.
This implementation is an extension of the Xgjoin operator
(see ANAPSID https://github.com/anapsid/anapsid).

The intermediate results are stored in queues and processed incrementally.

@author: Maribel Acosta
"""

from multiprocessing import Value
from Queue import Empty
from operatorstructures import Tuple, Record, RJTTail
from time import time
from random import randint

class Fjoin(object):

    def __init__(self, id_operator, variables, eddies, eddy=None):
        self.left_table = dict()
        self.right_table = dict()
        self.id_operator = id_operator
        #self.vars = set(variables)
        self.vars = variables
        self.eof = Tuple("EOF", 0, 0, set(), self.id_operator)
        self.eddies = eddies
        #if eddy:
        #    self.eddy = eddy
        #else:
            #self.eddy = randint(1, self.eddies)


        self.eddy = randint(1, self.eddies)
        self.left = None
        self.right = None
        self.qresults = None
        self.probing = Value('i', 1)
        self.independent_inputs = 2

    def to_dict(self):
        return {
            "type": str(self),
            "id_operator" : self.id_operator,
            "variables": list(self.vars),
            "eddies" : self.eddies,
            "eddy" : self.eddy
        }


    def __str__(self):
        return str("Fjoin")


    @staticmethod
    def symmetric():
        return True

    def execute(self, inputs, out):

        # Initialize input and output queues.
        self.left = inputs[0]
        self.right = inputs[1]
        self.qresults = out

        # Get the tuples from the input queues.
        while True:

            self.probing.value = 1
            # Try to get and process tuple from left queue.
            try:
                tuple1 = self.left.get(False)
                #print("{}: {}".format("Left", tuple1))
                self.stage1(tuple1, self.left_table, self.right_table)

            except Empty:
                # Empty: in tuple1 = self.left.get(False), when the queue is empty.
                #logging.info("Eddy {} Operator {} left queue is empty".format(self.eddy ,self.id_operator))
                self.probing.value = 0
                pass
            except TypeError:
                # TypeError: in resource = resource + tuple[var], when the tuple is "EOF".
                pass
            except IOError:
                # IOError: when a tuple is received, but the alarm is fired.
                pass

            # Try to get and process tuple from right queue.
            try:
                tuple2 = self.right.get(False)
                #print("{}: {}".format("Right", tuple2))
                #self.probing.value = 1
                self.stage1(tuple2, self.right_table, self.left_table)
                #self.probing.value = 0

            except Empty:
                # Empty: in tuple2 = self.right.get(False), when the queue is empty.
                #logging.info("Eddy {} Operator {} right queue is empty".format(self.eddy ,self.id_operator))
                self.probing.value = 0
                pass
            except TypeError:
                # TypeError: in resource = resource + tuple[var], when the tuple is "EOF".
                pass
            except IOError:
                # IOError: when a tuple is received, but the alarm is fired.
                pass

    # Stage 1: While one of the sources is sending data.
    def stage1(self, tuple1, tuple_rjttable, other_rjttable):


        # Get the value(s) of the join variable(s) in the tuple.
        resource = ''
        if tuple1.data != "EOF":
            for var in self.vars:
                try:
                    resource = resource + str(tuple1.data[var])
                    #print("{}: {}".format(self.id_operator, str(tuple1.data)))
                except Exception as e:
                    print(self.vars)
                    print(var)
                    print(tuple1.data)
                    print(self.id_operator)
                    print(tuple1.ready, tuple1.done, tuple1.sources, tuple1.from_operator, tuple1.to_operator)
                    print(e)
                    raise e
        else:
            resource = "EOF"

        # Probe the tuple against its RJT table.
        probe_ts = self.probe(tuple1, resource, tuple_rjttable)

        # Create the records.
        record = Record(tuple1, probe_ts, time(), float("inf"))

        # Insert the record in the corresponding RJT table.
        if resource in other_rjttable:
            other_rjttable.get(resource).updateRecords(record)
            other_rjttable.get(resource).setRJTProbeTS(probe_ts)
        else:
            tail = RJTTail(record, probe_ts)
            other_rjttable[resource] = tail

    # Stage 2: Executed when one source becomes blocked.
    def stage2(self, signum, frame):
        pass

    # Stage 3: Finalizes the join execution. It is fired when both sources has sent all the data.
    def stage3(self):
        return

    def probe(self, tuple1, resource, rjttable):

        # Probe a tuple against its corresponding table.
        probe_ts = time()

        # If the resource is in the table, produce results.
        if resource in rjttable:
            rjttable.get(resource).setRJTProbeTS(probe_ts)
            list_records = rjttable[resource].records

            # For each matching solution mapping, generate an answer.
            for record in list_records:
                if resource != "EOF":
                    # Merge solution mappings.
                    data = {}
                    data.update(record.tuple.data)
                    data.update(tuple1.data)
                else:
                    data = "EOF"

                # Update ready and done vectors.
                ready = record.tuple.ready | tuple1.ready
                done = record.tuple.done | tuple1.done | pow(2, self.id_operator)
                sources = list(set(record.tuple.sources) | set(tuple1.sources))

                # Create tuple.
                res = Tuple(data, ready, done, sources, self.id_operator)

                #print(ready, done)

                # Send tuple to eddy operators.
                self.qresults[self.eddy].put(res)

        return probe_ts
