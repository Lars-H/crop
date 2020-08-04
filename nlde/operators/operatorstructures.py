"""
Created on Jul 10, 2011

Implements the structures and functions used by the physical operators.

@author: Maribel Acosta
"""


class Tuple(object):
    """
    Represents a Tuple (solution mapping with signature).
    It is composed by the data (the SPARQL solution mapping),
    ready (operators that should be evaluated),
    done (operators already evaluated), and
    sources (where the tuple came from).
    """

    def __init__(self, data, ready, done, sources, from_operator=-1, to_operator=-1):
        self.data = data
        self.ready = ready
        self.done = done
        self.sources = sources
        self.from_operator = from_operator
        self.to_operator = to_operator

    def get_operators(self):
        int_type = self.ready - self.done
        
        res = []
        while int_type > 0:
            res.insert(0, int_type.bit_length()-1)
            int_type -= pow(2,int_type.bit_length()-1)
        return res

    def __str__(self):
        return str(self.data)


class Record(object):
    """
    Represents a structure that is inserted into the hash table.
    It is composed by a tuple, probeTS (timestamp when the tuple was probed)
    and insertTS (timestamp when the tuple was inserted in the table).
    """

    def __init__(self, tuple, probe_ts, insert_ts, flush_ts):
        self.tuple = tuple
        self.probe_ts = probe_ts
        self.insert_ts = insert_ts
        self.flush_ts = flush_ts
        

class RJTTail(object):
    """
    Represents the tail of a RJT.
    It is composed by a list of records and rjtprobeTS 
    (timestamp when the last tuple in the RJT was probed).
    """

    def __init__(self, record, rjt_probe_ts):
        self.records = [record]
        self.rjtProbeTS = rjt_probe_ts
        self.flushTS = float("inf")
        
    def updateRecords(self, record):
        self.records.append(record)
        
    def setRJTProbeTS(self, rjtProbeTS):
        self.rjtProbeTS = rjtProbeTS


def bitCount(int_type):
    """
    Counting bits set, Brian Kernighan's way.
    Source: http://wiki.python.org/moin/BitManipulation
    """
    count = 0
    while int_type:
        int_type &= int_type - 1
        count += 1
    return count
