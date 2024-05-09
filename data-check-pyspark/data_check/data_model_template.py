# jinja2.Template(template_sql).render(**arguments)

TEMPLATE_UNIQUENESS = """
{#-
  src (str) : source table
  keys (list[str]) : columns composing a unique key
-#}
with _result_full as (
  select
    {%- for x in keys %}
    {{ x }},
    {%- endfor %}
    count(*) as _cardinality
  from {{ src }}
  group by
    {%- for x in keys %}
    {{ x }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
)
select * from _result_full
where _cardinality > 1;
"""

TEMPLATE_DENORMALIZATION = """
{#-
  src (str) : source table
  group (list[str]) : columns defining a group
  values (list[str]) : columns that should have always the same value within a group
-#}
with _src_grouped as (
  select
    {%- for x in group %}
    {{ x }},
    {%- endfor %}
    {%- for x in values %}
    array_sort(transform(collect_set(struct({{ x }})), s -> s.{{ x }})) as {{ x }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
  from {{ src }}
  group by
    {%- for x in group %}
    {{ x }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
),
_result_full as (
  select
    {%- for x in group %}
    {{ x }},
    {%- endfor %}
    {%- for x in values %}
    case when size({{ x }}) > 1 then to_json({{ x }}) end as {{ x }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
  from _src_grouped
)
select * from _result_full
where
  {%- for x in values %}
  {{ x }} is not null {%- if not loop.last %} or {%- endif %}
  {%- endfor %};
"""

TEMPLATE_RELATIONSHIP = """
{#-
  src (str) : source table
  ref (str) : reference table
  keys (list[str]) : columns composing a unique source key
  join (list[str]) : columns used for joining tables
  semantic (str) : how many reference rows match the source row (exactly_one, at_most_one, at_least_one, none)
-#}
with _ref_prep as (
  select *, 1 as _ref_exists
  from {{ ref }}
),
_result_full as (
  select
    {%- for x in (keys+join)|unique %}
    src.{{ x }},
    {%- endfor %}
    count(_ref_exists) as _cardinality
  from {{ src }} src
  left join _ref_prep ref on
    {%- for x in join %}
    src.{{ x }} <=> ref.{{ x }} {%- if not loop.last %} and {%- endif %}
    {%- endfor %}
  group by
    {%- for x in (keys+join)|unique %}
    src.{{ x }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
)
select * from _result_full
where _cardinality {{ 
  {
    "exactly_one": "!= 1", 
    "at_most_one": "> 1", 
    "at_least_one": "< 1", 
    "none": ">= 1"
  }[semantic] 
}};
"""

TEMPLATE_CONSISTENCY = """
{#-
  src (str) : source table
  ref (str) : reference table
  join (list[str]) : columns used for joining tables
  values (list[str]) : columns that should have the same value in source and reference for joined rows
-#}
with _result_full as (
  select
    {%- for x in join %}
    src.{{ x }},
    {%- endfor %}
    {%- for x in values %}
    case
      when not(src.{{ x }} <=> ref.{{ x }})
      then to_json(array(src.{{ x }}, ref.{{ x }}))
    end as {{ x }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
  from {{ src }} src
  inner join {{ ref }} ref on
    {%- for x in join %}
    src.{{ x }} <=> ref.{{ x }} {%- if not loop.last %} and {%- endif %}
    {%- endfor %}
)
select * from _result_full
where
  {%- for x in values %}
  {{ x }} is not null {%- if not loop.last %} or {%- endif %}
  {%- endfor %};
"""

TEMPLATE_ACCURACY = """
{#-
  src (str) : source table
  ref (str) : reference table
  keys (list[str]) : columns composing a unique key (used for joining)
  exact (Optional[list[str]]) : columns that should have exactly the same value in source and reference for joined rows
  approx (Optional[dict[str, str]]) : columns that should have approximately the same value in source and reference for joined rows
    - dict key is a column name
    - dict value is a tolerance
-#}
with _src_prep as (
  select *, true as _exists
  from {{ src }}
),
_ref_prep as (
  select *, true as _exists
  from {{ ref }}
),
_result_full as (
  select
    {%- for x in keys %}
    nvl(src.{{ x }}, ref.{{ x }}) as {{ x }},
    {%- endfor %}
    case
      when not(src._exists <=> ref._exists)
      then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
    end as _exists
    {%- for x in exact %},
    case
      when not(src.{{ x }} <=> ref.{{ x }})
      then to_json(array(src.{{ x }}, ref.{{ x }}))
    end as {{ x }}
    {%- endfor %}
    {%- for x in approx %},
    case
      when not(
        (src.{{ x }} is null and ref.{{ x }} is null) or
        nvl(abs(src.{{ x }} - ref.{{ x }}) <= {{ approx[x] }}, false)
      )
      then to_json(array(src.{{ x }}, ref.{{ x }}))
    end as {{ x }}
    {%- endfor %}
  from _src_prep src
  full outer join _ref_prep ref on
    {%- for x in keys %}
    src.{{ x }} <=> ref.{{ x }} {%- if not loop.last %} and {%- endif %}
    {%- endfor %}
)
select * from _result_full
where
  _exists is not null
  {%- for x in exact %} or
  {{ x }} is not null
  {%- endfor %}
  {%- for x in approx %} or
  {{ x }} is not null
  {%- endfor %};
"""

TEMPLATE_ACCURACY_SORTED = """
{#-
  src (str) : source table
  ref (str) : reference table
  sort (list[str]) : columns used for sorting rows withing a group, typically it shoult be a subset of exact and approx
  group (Optional[list[str]]) : columns defining a group
  exact (Optional[list[str]]) : columns that should have exactly the same value in source and reference for joined rows
  approx (Optional[dict[str, str]]) : columns that should have approximately the same value in source and reference for joined rows
    - dict key is a column name
    - dict value is a tolerance
-#}
with _src_prep as (
  select 
    *, 
    true as _exists,
    row_number() over(
      {%- if group %}
      partition by
        {%- for x in group %}
        {{ x }} {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
      {%- endif %}
      order by
        {%- for x in sort %}
        {{ x }} {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    ) as _rank
  from {{ src }}
),
_ref_prep as (
  select 
    *, 
    true as _exists,
    row_number() over(
      {%- if group %}
      partition by
        {%- for x in group %}
        {{ x }} {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
      {%- endif %}
      order by
        {%- for x in sort %}
        {{ x }} {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    ) as _rank
  from {{ ref }}
),
_result_full as (
  select
    {%- for x in group %}
    nvl(src.{{ x }}, ref.{{ x }}) as {{ x }},
    {%- endfor %}
    nvl(src._rank, ref._rank) as _rank,
    case
      when not(src._exists <=> ref._exists)
      then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
    end as _exists
    {%- for x in exact %},
    case
      when not(src.{{ x }} <=> ref.{{ x }})
      then to_json(array(src.{{ x }}, ref.{{ x }}))
    end as {{ x }}
    {%- endfor %}
    {%- for x in approx %},
    case
      when not(
        (src.{{ x }} is null and ref.{{ x }} is null) or
        nvl(abs(src.{{ x }} - ref.{{ x }}) <= {{ approx[x] }}, false)
      )
      then to_json(array(src.{{ x }}, ref.{{ x }}))
    end as {{ x }}
    {%- endfor %}
  from _src_prep src
  full outer join _ref_prep ref on
    {%- for x in group %}
    src.{{ x }} <=> ref.{{ x }} and
    {%- endfor %}
    src._rank <=> ref._rank
)
select * from _result_full
where
  _exists is not null
  {%- for x in exact %} or
  {{ x }} is not null
  {%- endfor %}
  {%- for x in approx %} or
  {{ x }} is not null
  {%- endfor %};
"""
