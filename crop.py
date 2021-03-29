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
from multiprocessing import active_children
from time import time
import yaml

from nlde.engine.eddynetwork import EddyNetwork
from nlde.policy.nopolicy import NoPolicy

from crop.query_plan_optimizer import get_optimizer

def get_options():

    parser = argparse.ArgumentParser(description="CROP: An nLDE-based TPF Client supporting different query planning "
                                                 "approaches and polymorphic join operators.")

    # nLDE arguments.
    parser.add_argument("-s", "--sources",
                        help="URL of the triple pattern fragment servers (required)", nargs="+", required=True)
    parser.add_argument("-c", "--config",
                        help="Configuration YAML file. Including options for execution engine, planner, "
                             "and operators (required)",
                        required=True)
    parser.add_argument("-f", "--queryfile",
                        help="file name of the SPARQL query (required)", required=True)
    parser.add_argument("-o", "--output",
                        help="Format of the output (optional)",
                        choices=["y", "f", "dief", "all"],
                        default=None)
    parser.add_argument("-l", "--log",
                        help="Logging configuration (optional)",
                        choices=["INFO", "DEBUG"])
    parser.add_argument('-p', "--planning",
                        help="Just obtain the plan. Do not execute the plan (optional)",
                        action='store_true')


    args = parser.parse_args()
    parsed_args = vars(args)

    config_fn= parsed_args.get("config")
    config_dict = yaml.load(open(config_fn), Loader=yaml.FullLoader)
    config_dict.update(parsed_args)

    if args.log == "INFO" or args.log == "ALL":
        logger = logging.getLogger("nlde_logger")
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('logs/nlde.log')
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

    if args.log == "DEBUG" or args.log == "ALL":
        if args.log != "ALL":
            logging.getLogger("nlde_logger").setLevel(logging.WARNING)

        logger = logging.getLogger("nlde_debug")
        logger.setLevel(logging.DEBUG)

    elif not args.log == "INFO":
        logger = logging.getLogger("nlde_logger")
        logger.setLevel(logging.WARNING)


    new_sources = []
    for source in args.sources:
        if not "@" in source:
            source = "tpf@{}".format(source)
        new_sources.append(source)
    config_dict['sources'] = new_sources

    config_dict['execute'] =  not args.planning
    return config_dict


class NLDE(object):


    def __init__(self, **kwargs):

        self.query_id = ""
        self.network = None
        self.p_list = None
        self.config_file = kwargs.get("config")
        self.execute_plan = kwargs.get("execute", True)

        self.eddies = kwargs.get("nLDE", {}).get("eddies", 2)
        self.sources = kwargs.get("sources")

        kwargs['crop']['sources'] = self.sources
        self.optimizer_options = kwargs['crop']
        self.optimizer_options['operators'] = kwargs.get("operators")
        self.optimizer_options.update(kwargs.get('federation', {}))


        self.optimizer = get_optimizer(**self.optimizer_options)
        self.queryfile = kwargs.get("queryfile")
        self.output = kwargs.get("output", None)
        # Open query from file.
        if self.queryfile:
            self.query = open(self.queryfile).read()
            self.query_id = self.queryfile[self.queryfile.rfind("/") + 1:]
            self.query_dir = self.queryfile[:self.queryfile.rfind("/") + 1]

        else:
            sys.exit("No query provided.")

        # Set routing policy.
        # Set no routing policy
        self.policy = NoPolicy()

        # Set execution variables.
        self.init_time = None
        self.time_first = None
        self.time_total = None
        self.timeout = kwargs.get("nLDE", {}).get("timeout", None)
        self.card = 0
        self.xerror = ""



    def execute(self):
        logger = logging.getLogger("nlde_logger")

        if logger:
            logger.info('START %s', self.query_id)
            fhandler = logging.FileHandler('logs/{}.log'.format(self.query_id), 'w')
            fhandler.setLevel(logging.INFO)
            logger.addHandler(fhandler)

        # Initialization timestamp.
        self.init_time = time()
        self.time_last = 0

        # Create eddy network.
        if self.query:
            network = EddyNetwork(query=self.query, policy=self.policy, sources=self.sources,
                                  n_eddy=self.eddies, optimizer=self.optimizer)
        else:
            sys.exit("Could not create Eddy Network")

        # Set execution timeout
        #
        if self.timeout:
            signal.signal(signal.SIGALRM, network.stop_execution)
            signal.alarm(self.timeout)

        self.network = network
        self.p_list = self.network.p_list
        if self.execute_plan:
            self.execute_with_output(network)

        # For experiments: enable
        if self.output == "all":
            self.extended_summary(network)
        self.basic_summary()


    def execute_with_output(self, network):

        results = None
        if self.output == "f":
            results = []
            output_dir = "results"
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            out_fn = "{}/{}_{}_results.json".format(output_dir,self.query_id.replace(".rq", ""), self.optimizer)

        self.card  = 0


        for result in network.execute_standalone(network.plan):
            if self.card == 0:
                self.time_first = time() - self.init_time

            # print result
            if self.output == "y":
                print (result)

            elif self.output == "dief":
                t = time() - self.init_time
                print (self.query_id + "\t" + str(result) + "\t" + str(t) + "\t" + str(self.card))

            elif self.output == "f" and results:
                results.append(result)

            self.card += 1

        self.time_total = time() - self.init_time

        if results:
            with open(out_fn, "w") as result_file:
                json.dump(results, result_file)



    # Final stats of execution.
    def basic_summary(self):
        print self.query_id + "\t" + str(self.time_first) + "\t" + str(self.time_total) + \
              "\t" + str(self.card) + "\t" + "\t"+ str(self.network.plan.total_requests) + "\t"+ str(self.config_file) + str(
            self.xerror)

    def extended_summary(self, network, timeout=False):

        result_dct = {
            "query": self.query_id,
            "runtime_s": self.time_total,
            "answers": self.card,
            "optimization_time_s": network.optimization_time,
            "triple_pattern_count": network.triple_pattern_cnt,
            "source" : self.sources[0],
            "config_file" : self.config_file,
            "timeout_reached" : self.network.reached_timeout,
            "timeout_s" : self.timeout
        }
        result_dct.update(network.optimizer.params_dct)
        result_dct.update(self.optimizer_options['operators'])


        # Runtime stats:
        if network.reached_timeout:
            # If the timeout is reached, we need to get the stats from the logs
            request_count, operator_stats = self.stats_from_logs()
            self.network.plan.logical_plan_stats(operator_stats)
            result_dct['requests'] = request_count
            self.network.plan.execution_requests = request_count - self.network.plan.planning_requests
            if self.card == 0:
                self.time_first = self.timeout

            result_dct['q_errors'] = {}
        else:
            # Otherwise, the stats have been sent
            result_dct['requests'] = network.plan.total_requests
            result_dct['q_errors'] = network.plan.q_errors

        # Add Plan Dict
        result_dct['plan_dict'] = network.plan.json_dict

        # Cost / Robustness Stats
        cost_model_params = network.optimizer.cost_model.params_dct
        result_dct.update({
            "bc_cost_p_star" : network.plan.cost(network.optimizer.cost_model),
            "ac_cost_p_star" : network.plan.average_cost(network.optimizer.robust_model)
        })
        result_dct.update(cost_model_params)
        #network.plan.cost(network.optimizer.cost_model)
        result_dct["robustness_p_star"] = result_dct.get("robustness_p_star",
                                                           result_dct['bc_cost_p_star'] / result_dct[
                                                           'ac_cost_p_star'])

        result_dct['first_result_elpased_s'] = self.time_first

        print result_dct


    # Finalize execution: kill sub-processes.
    def finalize(self):
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

    def stats_from_logs(self):
        from ast import literal_eval
        request = 0
        logger = logging.getLogger("nlde_logger")
        operator_stats = {}
        if logger:
            with open('logs/{}.log'.format(self.query_id), "r") as infile:
                try:
                    for line in infile.readlines():
                        if "request" in line:
                            dct = literal_eval(line)
                            request += dct['requests']
                        if "'switched':" in line:
                            dct = literal_eval(line)
                            operator_stats.update(dct)
                    return request, operator_stats
                except Exception as e:
                    print e
                    return -1, operator_stats
        return -1, operator_stats


if __name__ == '__main__':

    options = get_options()
    nlde = NLDE(**options)
    try:
        nlde.execute()
    except KeyboardInterrupt:
        pass
    nlde.finalize()
    sys.exit(0)

