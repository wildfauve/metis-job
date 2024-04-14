from metis_fn import monad

import metis_job

from tests.shared import init_state_spy


@metis_job.initialiser_register(order=1)
def init_thing():
    init_state_spy.InitState().add_state("random-initialiser-1-run")
    return monad.Right('OK')