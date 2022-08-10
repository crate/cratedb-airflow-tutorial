"""
Column Level Checks
"""
COL_CHECKS = [
    {"time": {
        "null_check": {"equal_to": 0},
        "unique_check": {"equal_to": 0}
    }},
    {"use_kw": {
        "null_check": {"equal_to": 0}
    }},
    {"gen_kw": {
        "null_check": {"equal_to": 0}
    }},
    {"temperature": {
        "min": {"geq_to": -20},
        "max": {"less_than": 99}
    }},
    {"humidity": {
        "min": {"geq_to": 0},
        "max": {"less_than": 1}
    }}
]

"""
Table Level Checks
"""
TABLE_CHECKS = [
    {"row_count_check": {"check_statement": "COUNT(*) > 100000"}},
    {"total_usage_check": {"check_statement": "dishwasher + home_office + "
                           + "fridge + wine_cellar + kitchen + "
                           + "garage_door + microwave + barn + "
                           + " well + living_room  <= house_overall"}}
]
