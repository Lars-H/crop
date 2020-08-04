#!/usr/bin/env python

"""
Created on Mar 25, 2015

@author: Maribel Acosta

Updated on Mar, 2020

@author: Anonymous

"""
import argparse
import os
import signal
import sys
import logging
import json
from multiprocessing import active_children, Queue
from time import time

from nlde.engine.eddynetwork import EddyNetwork
from nlde.policy.nopolicy import NoPolicy
from nlde.policy.ticketpolicy import TicketPolicy
from nlde.policy.uniformrandompolicy import UniformRandomPolicy
from crop.costmodel.crop_cost_model import CropCostModel


def get_options():

    parser = argparse.ArgumentParser(description="CROP: An nLDE-based TPF Client"
                                                 " with a cost model-based robust query plan optimizer")

    # nLDE arguments.
    parser.add_argument("-s", "--server",
                        help="URL of the triple pattern fragment server (required)")
    parser.add_argument("-f", "--queryfile",
                        help="file name of the SPARQL query (required, or -q)")
    parser.add_argument("-q", "--query",
                        help="SPARQL query (required, or -f)")
    parser.add_argument("-r", "--printres",
                        help="format of the output results",
                        choices=["y", "n", "f" ,"all"],
                        default="y")
    parser.add_argument("-e", "--eddies",
                        help="number of eddy processes to create",
                        type=int,
                        default=2)
    parser.add_argument("-p", "--policy",
                        help="routing policy used by eddy operators",
                        choices=["NoPolicy", "Ticket", "Random"],
                        default="NoPolicy")
    parser.add_argument("-t", "--timeout",
                        help="query execution timeout in seconds.",
                        type=int)
    parser.add_argument("-v", "--verbose",
                        help="print logging information",
                        choices=["INFO"])

    # CROP Specific Arguments

    # Cost model parameters
    parser.add_argument("-d", "--height_discount",
                        help="Discount for NLJ higher in the plan (Optional)",
                        type=float,
                        default=4.0)

    # Quey plan optimizer parameters
    parser.add_argument("-c", "--cost_threshold",
                        help="Cost threshold for the query plan optimizer(Optional)",
                        type=float,
                        default=0.3)
    parser.add_argument("-u", "--robust_threshold",
                        help="Robustness threshold for the query plan optimizer(Optional)",
                        type=float,
                        default=0.05)
    # IDP Parameters
    parser.add_argument("-k", "--k",
                        help="Parameter k in IDP (Optional)",
                        type=int,
                        default=4)
    # IDP Parameters
    parser.add_argument("-a", "--adaptive_k",
                        help="Adaptive k (Optional)",
                        choices=["True", "False"])
    parser.add_argument("-b", "--top_t",
                        help="Top t plans to consider in each iteration in IDP (Optional)",
                        type=int,
                        default=5)


    args = parser.parse_args()

    # Handling mandatory arguments.
    err = False
    msg = []
    if not args.server:
        err = True
        msg.append("error: no server specified. Use argument -s to specify the address of a server.")

    if not args.queryfile and not args.query and not args.json_plan:
        err = True
        msg.append("error: no query specified. Use argument -f, -q or -j to specify a query or a query plan.")

    if args.verbose == "INFO":
        logger = logging.getLogger("nlde_logger")
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('logs/nlde.log')
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

    else:
        logger = logging.getLogger("nlde_logger")
        logger.setLevel(logging.WARNING)

    #if args.verbose == "DEBUG":
    #    logging.basicConfig(level=logging.DEBUG)

    if err:
        parser.print_usage()
        print ("\n".join(msg))
        sys.exit(1)

    return args


class NLDE(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

        self.query_id = ""
        self.network = None
        self.p_list = None
        self.res = Queue()

        self.cost_model = CropCostModel(**kwargs)
        self.robust_model = CropCostModel(**kwargs)

        # IDP Parameters
        self.k_idp = kwargs.get("k")
        self.top_t_idp = kwargs.get("top_t")
        self.adaptive_k = kwargs.get("adaptive_k") != "False"

        # Query Plan Optimizer Parameter
        self.cost_threshold = kwargs.get("cost_threshold")
        self.robust_threshold = kwargs.get("robust_threshold")


        # Open query from file.
        if hasattr(self, "queryfile") and self.queryfile:
            self.query = open(self.queryfile).read()
            self.query_id = self.queryfile[self.queryfile.rfind("/") + 1:]
            self.query_dir = self.queryfile[:self.queryfile.rfind("/") + 1]


        # Set Optimization Option
        self.optimization = kwargs.get("optimization", "CROP")

        # Set routing policy.
        if self.policy == "NoPolicy":
            self.policy = NoPolicy()
        elif self.policy == "Ticket":
            self.policy = TicketPolicy()
        elif self.policy == "Random":
            self.policy = UniformRandomPolicy()

        # Set execution variables.
        self.init_time = None
        self.time_first = None
        self.time_total = None
        self.card = 0
        self.xerror = ""

        # Set execution timeout.
        if self.timeout:
            signal.signal(signal.SIGALRM, self.call_timeout)
            signal.alarm(self.timeout)


    def execute(self):
        logger = logging.getLogger("nlde_logger")

        if logger:
            logger.info('START %s', self.query_id)
            fhandler = logging.FileHandler('logs/{}.log'.format(self.query_id), 'w')
            fhandler.setLevel(logging.INFO)
            logger.addHandler(fhandler)

        # Initialization timestamp.
        self.init_time = time()

        # Create eddy network.
        if hasattr(self, "query") and self.query:
            network = EddyNetwork(query=self.query, policy=self.policy, source=self.server,
                                  n_eddy=self.eddies, optimization=self.optimization,
                                  cost_model=self.cost_model, robust_model=self.robust_model, k=self.k_idp, top_t=self.top_t_idp,
                                  cost_threshold = self.cost_threshold, robust_threshold=self.robust_threshold,
                                  adaptive_k = self.adaptive_k)

        else:
            sys.exit("Could not create Eddy Network")

        self.network = network

        self.p_list = network.p_list


        if self.printres == "y":
            self.print_solutions(network)
        elif self.printres == "all":
            self.print_all(network)
        elif self.printres == "f":
            self.solutions_to_file(network)
        else:
            self.print_basics(network)

        if self.optimization in ["CROP"]:
            self.extended_summary(network)
        else:
            self.basic_summary()


    # Print only basic stats, but still iterate over results.
    def print_basics(self, network):

        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get(True)
            if ri.data == "EOF":
                count = count + 1
            else:
                self.card = self.card + 1

        self.time_total = time() - self.init_time

    # Print only solution mappings.
    def print_solutions(self, network):

        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1
            print (str(ri.data))

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get(True)
            if ri.data == "EOF":
                count = count + 1
            else:
                self.card = self.card + 1
                print (str(ri.data))

        self.time_total = time() - self.init_time

    # Print all stats for each solution mapping.
    def print_all(self, network):

        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1
            print (self.query_id + "\t" + str(ri.data) + "\t" + str(self.time_first) + "\t" + str(self.card))

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get (True)
            if ri.data == "EOF":
                count = count + 1
            else:
                self.card = self.card + 1
                t = time() - self.init_time
                print (self.query_id + "\t" + str(ri.data) + "\t" + str(t) + "\t" + str(self.card))

        self.time_total = time() - self.init_time

    # Print all stats for each solution mapping.
    def solutions_to_file(self, network):

        results = []
        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1
            results.append(ri.data)
            print(ri.data)

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get(True)
            if ri.data == "EOF":
                count = count + 1
            else:
                self.card = self.card + 1
                results.append(ri.data)
                #if len(ri.data.keys()) != 3:
                print (len(ri.data.keys()))

        self.time_total = time() - self.init_time

        output_dir = "results"
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        out_fn = "{}/{}_{}_results.json".format(output_dir,self.query_id.replace(".rq", ""), self.optimization.lower())
        with open(out_fn, "w") as result_file:
            json.dump(results, result_file)


    # Final stats of execution.
    def basic_summary(self):
        print (self.query_id + "\t" + str(self.time_first) + "\t" + str(self.time_total) + \
              "\t" + str(self.card) + "\t" + str(self.xerror))

    def experiment_summary(self, network, timeout=False):

        result_dct = {}
        t_total = self.time_total
        t_first = self.time_first
        opt_time = network.optimization_time
        est_card = network.plan.cardinality(network.cost_model)
        cost = network.plan.cost(network.cost_model)
        robustness = network.plan.average_cost(network.robust_model)
        bushy = network.plan.is_bushy

        if timeout:
            t_total = self.timeout

        idp_params = network.idp_optimizer.params
        cost_model_params = network.cost_model.params
        params = [
            self.query_id, t_first, t_total, self.card, est_card, self.optimization, opt_time, cost, robustness, bushy, network.triple_pattern_cnt
        ]
        params.extend(idp_params)
        params.extend(cost_model_params)
        request_count = self.get_request_count()
        params.append(request_count)
        params.append(self.query_dir)
        print ("\t".join(str(param) for param in params))

    def extended_summary(self, network, timeout=False):


        idp_params = network.idp_optimizer.params_dct
        cost_model_params = network.cost_model.params_dct

        result_dct = {
            "query" : self.query_id,
            "runtime_s" : self.time_total,
            "answers" : self.card,
            "optimization_time_s" : network.optimization_time,
            "bc_cost_p_star" : network.plan.cost(network.cost_model),
            "ac_cost_p_star" : network.plan.average_cost(network.robust_model),
            "triple_pattern_count" : network.triple_pattern_cnt
        }
        result_dct.update(idp_params)
        result_dct.update(cost_model_params)
        request_count = self.get_request_count()
        if request_count == 0:
            request_count = "NA"
        result_dct['requests'] = request_count

        print result_dct

    # Timeout was fired.
    def call_timeout(self, sig, err):
        self.time_total = time() - self.init_time
        self.finalize()
        if self.optimization in ["CROP"]:
            self.experiment_summary(self.network, True)
        else:
            self.basic_summary()
        sys.exit(1)

    # Finalize execution: kill sub-processes.
    def finalize(self):

        self.res.close()

        while not self.p_list.empty():
            pid = self.p_list.get()
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError as e:
                pass

        for p in active_children():
            try:
                p.terminate()
            except OSError as e:
                pass

        logger = logging.getLogger("nlde_logger")
        if logger:
            logger.info('END %s', self.query_id)

    def get_request_count(self):
        from ast import literal_eval
        request = 0
        logger = logging.getLogger("nlde_logger")
        if logger:
            with open('logs/{}.log'.format(self.query_id), "r") as infile:
                for line in infile.readlines():
                    if "request" in line:
                        dct = literal_eval(line)
                        request += dct['requests']
                return request
        return -1


if __name__ == '__main__':

    options = get_options()
    nlde = NLDE(**vars(options))
    nlde.execute()
    nlde.finalize()
    sys.exit(0)