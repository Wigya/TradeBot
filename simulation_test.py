from datetime import timedelta
import pandas as pd
import numpy as np
from simulation import simulation
import pytest
"""Contains tests for BaseCreator class.

Examples
--------
pytest -q nazwa_pliku.py -v --pdb --maxfail=100

"""

@pytest.mark.parametrize('inputs', [
    ('2018-09-05 14:32:00', 25, 1632.12),
    ('2020-09-05 15:32:00', 15, 1532.12),
    ('2020-09-05 13:32:00', 35, 1132.12),
    ('2020-09-05 11:32:00', 55, 1932.12),
])
#def simulation_test(inputs):
#    actual_result = simulation()
#    transaction_date, transaction_days, verified_result = inputs
#    pd.DataFrame(transaction_date)
#    actual_result = simulation(transaction_date, transaction_days)
#    assert actual_result == verified_result


#simulation_test()
