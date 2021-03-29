from crop.query_plan_optimizer.ldff_optimizer import LDFF_Optimizer
from crop.query_plan_optimizer.nlde_optimizer import nLDE_Optimizer
from crop.query_plan_optimizer.idp_optimizer import IDP_Optimizer
from crop.costmodel.crop_cost_model import CropCostModel

def get_optimizer(**kwargs):

    sources = kwargs.get("sources", "")
    n_eddy = kwargs.get("eddies", 2)
    optimizer_name = kwargs.get("optimizer", "idp").lower()
    operator_config = kwargs.get("operators", {})

    # Cost Model, Robust Model
    cost_model = CropCostModel(**kwargs)
    robust_model = CropCostModel(**kwargs)

    if optimizer_name == "idp" or optimizer_name == "crop":

        ## IDP Optimizer Setup
        k = kwargs.get("k")
        top_t = kwargs.get("top_t")
        adaptive_k = kwargs.get("adaptive_k")
        enable_robustplan = True
        robustness_threshold = kwargs.get("robust_threshold")
        cost_threshold = kwargs.get("cost_threshold")
        optimizer = IDP_Optimizer(eddies=n_eddy, sources=sources, cost_model=cost_model,
                                       robust_model=robust_model, k=k, top_t=top_t, adaptive_k=adaptive_k,
                                       enable_robustplan=enable_robustplan,
                                       robustness_threshold=robustness_threshold, cost_threshold=cost_threshold,
                                  operator_config=operator_config)

    elif optimizer_name == "nlde":
        optimizer = nLDE_Optimizer(sources=sources, eddies=n_eddy, operator_config=operator_config)

    elif optimizer_name == "ldff" or optimizer_name == "left-linear":
        pbj = kwargs.get("pbj") != "False"

        decomposer =  kwargs.get("decomposer", False)
        prune_sources = kwargs.get("prune_sources", False)

        optimizer = LDFF_Optimizer(sources=sources, eddies=n_eddy, pbj=pbj, decomposer=decomposer,
                                   pruning=prune_sources, operator_config=operator_config)

    else:
        raise Exception("Invalid Optimizer Option")

    optimizer.cost_model = cost_model
    optimizer.robust_model = robust_model
    return optimizer