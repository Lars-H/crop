"""
Created on Jan 15, 2020

Physical operator that implements a UNION.

"""
from random import randint
from multiprocessing import Value  # , Queue
from Queue import Empty, Queue

import logging
logging.basicConfig(level=logging.INFO)

class Xunion(object):

    def __init__(self, id_operator, eddies):
        self.id_operator = id_operator
        self.left = None
        self.right = None
        self.qresults = None
        self.eddies = eddies
        self.eddy = randint(1, self.eddies)
        self.probing = Value('i', 1)
        self.left_wait = True
        self.right_wait = True
        self.independent_inputs = 1


    @property
    def cardinality(self):
        return self.left.compute_cardinality * self.right.compute_cardinality
    @property
    def selectivity(self):
        return 1

    # Executes the Xunion operator.
    def execute(self, inputs, out):
        # Initialize input and output queues.
        self.left = inputs[0]
        if inputs[1]:
            self.right = inputs[1]
        else:
            self.right = Queue()
            self.right_wait = False
        self.qresults = out

        # Get the tuples (solution mappings) from the input queue.
        while True:

            try:
                # Get next left tuple (solution mapping).
                tuple1 = self.left.get(self.left_wait)

                if tuple1.data != "EOF":
                    str_tuple = frozenset(tuple1.data.items())
                else:
                    self.left_wait = False

                tuple1.done = tuple1.done | pow(2, self.id_operator)
                tuple1.from_operator = self.id_operator
                self.qresults[self.eddy].put(tuple1)

            except Empty:
                pass
                #self.probing.value = 0

            try:
                # Get next right tuple (solution mapping).
                tuple1 = self.right.get(self.right_wait)

                if tuple1.data != "EOF":
                    str_tuple = frozenset(tuple1.data.items())
                else:
                    self.right_wait = False

                tuple1.done = tuple1.done | pow(2, self.id_operator)
                tuple1.from_operator = self.id_operator
                self.qresults[self.eddy].put(tuple1)

            except Empty:
                pass


            if self.left.empty() and self.right.empty():
                self.probing.value = 0