with element_rows as (
select unnest(elements) as elements from read_json_auto('input/idj_ctc_calculator_data.json')
),

expanded as (
select unnest(elements)
from element_rows
)

select ref, elementType,
    label,
    settings.expression as expression,
    showElement,
    regexp_extract_all(expression, '(#\d+)').list_distinct() as ref_deps,
    -- settings
from expanded