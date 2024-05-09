from jinja2 import Template
from textwrap import dedent

from data_check.data_model_template import *

def test_uniqueness():
    sql_rendered = Template(TEMPLATE_UNIQUENESS).render(
        src="order_details",
        keys=["order_id", "item_id"],
    )

    sql_expected = dedent("""
    with _result_full as (
      select
        order_id,
        item_id,
        count(*) as _cardinality
      from order_details
      group by
        order_id,
        item_id
    )
    select * from _result_full
    where _cardinality > 1;
    """).strip()

    assert sql_rendered == sql_expected, f"SQL queries are different"


def test_denormalization():
    sql_rendered = Template(TEMPLATE_DENORMALIZATION).render(
      src="promotion_details",
      group=["article_id", "supplier_id"],
      values=["category_name", "supplier_phone"],
    )
    
    sql_expected = dedent("""
    with _src_grouped as (
      select
        article_id,
        supplier_id,
        array_sort(transform(collect_set(struct(category_name)), s -> s.category_name)) as category_name,
        array_sort(transform(collect_set(struct(supplier_phone)), s -> s.supplier_phone)) as supplier_phone
      from promotion_details
      group by
        article_id,
        supplier_id
    ),
    _result_full as (
      select
        article_id,
        supplier_id,
        case when size(category_name) > 1 then to_json(category_name) end as category_name,
        case when size(supplier_phone) > 1 then to_json(supplier_phone) end as supplier_phone
      from _src_grouped
    )
    select * from _result_full
    where
      category_name is not null or
      supplier_phone is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"


def test_relationship():
    ## exactly_one
    sql_rendered = Template(TEMPLATE_RELATIONSHIP).render(
      src="order_details",
      ref="promotion_details",
      keys=["order_id", "article_id"],
      join=["article_id", "promotion_id"],
      semantic="exactly_one",
    )
    
    sql_expected = dedent("""
    with _ref_prep as (
      select *, 1 as _ref_exists
      from promotion_details
    ),
    _result_full as (
      select
        src.order_id,
        src.article_id,
        src.promotion_id,
        count(_ref_exists) as _cardinality
      from order_details src
      left join _ref_prep ref on
        src.article_id <=> ref.article_id and
        src.promotion_id <=> ref.promotion_id
      group by
        src.order_id,
        src.article_id,
        src.promotion_id
    )
    select * from _result_full
    where _cardinality != 1;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## at_most_one
    sql_rendered = Template(TEMPLATE_RELATIONSHIP).render(
      src="order_details",
      ref="promotion_details",
      keys=["order_id", "article_id"],
      join=["article_id", "promotion_id"],
      semantic="at_most_one",
    )
    
    sql_expected = dedent("""
    with _ref_prep as (
      select *, 1 as _ref_exists
      from promotion_details
    ),
    _result_full as (
      select
        src.order_id,
        src.article_id,
        src.promotion_id,
        count(_ref_exists) as _cardinality
      from order_details src
      left join _ref_prep ref on
        src.article_id <=> ref.article_id and
        src.promotion_id <=> ref.promotion_id
      group by
        src.order_id,
        src.article_id,
        src.promotion_id
    )
    select * from _result_full
    where _cardinality > 1;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## at_least_one
    sql_rendered = Template(TEMPLATE_RELATIONSHIP).render(
      src="order_details",
      ref="promotion_details",
      keys=["order_id", "article_id"],
      join=["article_id", "promotion_id"],
      semantic="at_least_one",
    )
    
    sql_expected = dedent("""
    with _ref_prep as (
      select *, 1 as _ref_exists
      from promotion_details
    ),
    _result_full as (
      select
        src.order_id,
        src.article_id,
        src.promotion_id,
        count(_ref_exists) as _cardinality
      from order_details src
      left join _ref_prep ref on
        src.article_id <=> ref.article_id and
        src.promotion_id <=> ref.promotion_id
      group by
        src.order_id,
        src.article_id,
        src.promotion_id
    )
    select * from _result_full
    where _cardinality < 1;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## none
    sql_rendered = Template(TEMPLATE_RELATIONSHIP).render(
      src="order_details",
      ref="promotion_details",
      keys=["order_id", "article_id"],
      join=["article_id", "promotion_id"],
      semantic="none",
    )
    
    sql_expected = dedent("""
    with _ref_prep as (
      select *, 1 as _ref_exists
      from promotion_details
    ),
    _result_full as (
      select
        src.order_id,
        src.article_id,
        src.promotion_id,
        count(_ref_exists) as _cardinality
      from order_details src
      left join _ref_prep ref on
        src.article_id <=> ref.article_id and
        src.promotion_id <=> ref.promotion_id
      group by
        src.order_id,
        src.article_id,
        src.promotion_id
    )
    select * from _result_full
    where _cardinality >= 1;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"


def test_consistency():
    sql_rendered = Template(TEMPLATE_CONSISTENCY).render(
      src="order_details",
      ref="promotion_details",
      join=["article_id", "promotion_id"],
      values=["category_name", "supplier_phone"],
    )
    
    sql_expected = dedent("""
    with _result_full as (
      select
        src.article_id,
        src.promotion_id,
        case
          when not(src.category_name <=> ref.category_name)
          then to_json(array(src.category_name, ref.category_name))
        end as category_name,
        case
          when not(src.supplier_phone <=> ref.supplier_phone)
          then to_json(array(src.supplier_phone, ref.supplier_phone))
        end as supplier_phone
      from order_details src
      inner join promotion_details ref on
        src.article_id <=> ref.article_id and
        src.promotion_id <=> ref.promotion_id
    )
    select * from _result_full
    where
      category_name is not null or
      supplier_phone is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"


def test_accuracy():
    ## keys, exact and approx
    sql_rendered = Template(TEMPLATE_ACCURACY).render(
      src="real_result",
      ref="expected_result",
      keys=["order_date", "category_id"],
      exact=["order_month", "order_counts"],
      approx={"sales_total": "1e-12", "sales_avg": "0.01"},
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select *, true as _exists
      from real_result
    ),
    _ref_prep as (
      select *, true as _exists
      from expected_result
    ),
    _result_full as (
      select
        nvl(src.order_date, ref.order_date) as order_date,
        nvl(src.category_id, ref.category_id) as category_id,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists,
        case
          when not(src.order_month <=> ref.order_month)
          then to_json(array(src.order_month, ref.order_month))
        end as order_month,
        case
          when not(src.order_counts <=> ref.order_counts)
          then to_json(array(src.order_counts, ref.order_counts))
        end as order_counts,
        case
          when not(
            (src.sales_total is null and ref.sales_total is null) or
            nvl(abs(src.sales_total - ref.sales_total) <= 1e-12, false)
          )
          then to_json(array(src.sales_total, ref.sales_total))
        end as sales_total,
        case
          when not(
            (src.sales_avg is null and ref.sales_avg is null) or
            nvl(abs(src.sales_avg - ref.sales_avg) <= 0.01, false)
          )
          then to_json(array(src.sales_avg, ref.sales_avg))
        end as sales_avg
      from _src_prep src
      full outer join _ref_prep ref on
        src.order_date <=> ref.order_date and
        src.category_id <=> ref.category_id
    )
    select * from _result_full
    where
      _exists is not null or
      order_month is not null or
      order_counts is not null or
      sales_total is not null or
      sales_avg is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## keys and exact
    sql_rendered = Template(TEMPLATE_ACCURACY).render(
      src="real_result",
      ref="expected_result",
      keys=["order_date", "category_id"],
      exact=["order_month", "order_counts"],
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select *, true as _exists
      from real_result
    ),
    _ref_prep as (
      select *, true as _exists
      from expected_result
    ),
    _result_full as (
      select
        nvl(src.order_date, ref.order_date) as order_date,
        nvl(src.category_id, ref.category_id) as category_id,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists,
        case
          when not(src.order_month <=> ref.order_month)
          then to_json(array(src.order_month, ref.order_month))
        end as order_month,
        case
          when not(src.order_counts <=> ref.order_counts)
          then to_json(array(src.order_counts, ref.order_counts))
        end as order_counts
      from _src_prep src
      full outer join _ref_prep ref on
        src.order_date <=> ref.order_date and
        src.category_id <=> ref.category_id
    )
    select * from _result_full
    where
      _exists is not null or
      order_month is not null or
      order_counts is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## keys and approx
    sql_rendered = Template(TEMPLATE_ACCURACY).render(
      src="real_result",
      ref="expected_result",
      keys=["order_date", "category_id"],
      approx={"sales_total": "1e-12", "sales_avg": "0.01"},
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select *, true as _exists
      from real_result
    ),
    _ref_prep as (
      select *, true as _exists
      from expected_result
    ),
    _result_full as (
      select
        nvl(src.order_date, ref.order_date) as order_date,
        nvl(src.category_id, ref.category_id) as category_id,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists,
        case
          when not(
            (src.sales_total is null and ref.sales_total is null) or
            nvl(abs(src.sales_total - ref.sales_total) <= 1e-12, false)
          )
          then to_json(array(src.sales_total, ref.sales_total))
        end as sales_total,
        case
          when not(
            (src.sales_avg is null and ref.sales_avg is null) or
            nvl(abs(src.sales_avg - ref.sales_avg) <= 0.01, false)
          )
          then to_json(array(src.sales_avg, ref.sales_avg))
        end as sales_avg
      from _src_prep src
      full outer join _ref_prep ref on
        src.order_date <=> ref.order_date and
        src.category_id <=> ref.category_id
    )
    select * from _result_full
    where
      _exists is not null or
      sales_total is not null or
      sales_avg is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## only keys
    sql_rendered = Template(TEMPLATE_ACCURACY).render(
      src="real_result",
      ref="expected_result",
      keys=["order_date", "category_id"],
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select *, true as _exists
      from real_result
    ),
    _ref_prep as (
      select *, true as _exists
      from expected_result
    ),
    _result_full as (
      select
        nvl(src.order_date, ref.order_date) as order_date,
        nvl(src.category_id, ref.category_id) as category_id,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists
      from _src_prep src
      full outer join _ref_prep ref on
        src.order_date <=> ref.order_date and
        src.category_id <=> ref.category_id
    )
    select * from _result_full
    where
      _exists is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"


def test_accuracy_sorted():
    ## group, sort, exact and approx
    sql_rendered = Template(TEMPLATE_ACCURACY_SORTED).render(
      src="real_result",
      ref="expected_result",
      group=["order_date", "category_id"],
      sort=["order_counts", "sales_total"],
      exact=["order_month", "order_counts"],
      approx={"sales_total": "1e-12", "sales_avg": "0.01"},
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          partition by
            order_date,
            category_id
          order by
            order_counts,
            sales_total
        ) as _rank
      from real_result
    ),
    _ref_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          partition by
            order_date,
            category_id
          order by
            order_counts,
            sales_total
        ) as _rank
      from expected_result
    ),
    _result_full as (
      select
        nvl(src.order_date, ref.order_date) as order_date,
        nvl(src.category_id, ref.category_id) as category_id,
        nvl(src._rank, ref._rank) as _rank,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists,
        case
          when not(src.order_month <=> ref.order_month)
          then to_json(array(src.order_month, ref.order_month))
        end as order_month,
        case
          when not(src.order_counts <=> ref.order_counts)
          then to_json(array(src.order_counts, ref.order_counts))
        end as order_counts,
        case
          when not(
            (src.sales_total is null and ref.sales_total is null) or
            nvl(abs(src.sales_total - ref.sales_total) <= 1e-12, false)
          )
          then to_json(array(src.sales_total, ref.sales_total))
        end as sales_total,
        case
          when not(
            (src.sales_avg is null and ref.sales_avg is null) or
            nvl(abs(src.sales_avg - ref.sales_avg) <= 0.01, false)
          )
          then to_json(array(src.sales_avg, ref.sales_avg))
        end as sales_avg
      from _src_prep src
      full outer join _ref_prep ref on
        src.order_date <=> ref.order_date and
        src.category_id <=> ref.category_id and
        src._rank <=> ref._rank
    )
    select * from _result_full
    where
      _exists is not null or
      order_month is not null or
      order_counts is not null or
      sales_total is not null or
      sales_avg is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## group, sort and exact
    sql_rendered = Template(TEMPLATE_ACCURACY_SORTED).render(
      src="real_result",
      ref="expected_result",
      group=["order_date", "category_id"],
      sort=["order_counts", "sales_total"],
      exact=["order_month", "order_counts"],
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          partition by
            order_date,
            category_id
          order by
            order_counts,
            sales_total
        ) as _rank
      from real_result
    ),
    _ref_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          partition by
            order_date,
            category_id
          order by
            order_counts,
            sales_total
        ) as _rank
      from expected_result
    ),
    _result_full as (
      select
        nvl(src.order_date, ref.order_date) as order_date,
        nvl(src.category_id, ref.category_id) as category_id,
        nvl(src._rank, ref._rank) as _rank,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists,
        case
          when not(src.order_month <=> ref.order_month)
          then to_json(array(src.order_month, ref.order_month))
        end as order_month,
        case
          when not(src.order_counts <=> ref.order_counts)
          then to_json(array(src.order_counts, ref.order_counts))
        end as order_counts
      from _src_prep src
      full outer join _ref_prep ref on
        src.order_date <=> ref.order_date and
        src.category_id <=> ref.category_id and
        src._rank <=> ref._rank
    )
    select * from _result_full
    where
      _exists is not null or
      order_month is not null or
      order_counts is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## group, sort and approx
    sql_rendered = Template(TEMPLATE_ACCURACY_SORTED).render(
      src="real_result",
      ref="expected_result",
      group=["order_date", "category_id"],
      sort=["order_counts", "sales_total"],
      approx={"sales_total": "1e-12", "sales_avg": "0.01"},
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          partition by
            order_date,
            category_id
          order by
            order_counts,
            sales_total
        ) as _rank
      from real_result
    ),
    _ref_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          partition by
            order_date,
            category_id
          order by
            order_counts,
            sales_total
        ) as _rank
      from expected_result
    ),
    _result_full as (
      select
        nvl(src.order_date, ref.order_date) as order_date,
        nvl(src.category_id, ref.category_id) as category_id,
        nvl(src._rank, ref._rank) as _rank,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists,
        case
          when not(
            (src.sales_total is null and ref.sales_total is null) or
            nvl(abs(src.sales_total - ref.sales_total) <= 1e-12, false)
          )
          then to_json(array(src.sales_total, ref.sales_total))
        end as sales_total,
        case
          when not(
            (src.sales_avg is null and ref.sales_avg is null) or
            nvl(abs(src.sales_avg - ref.sales_avg) <= 0.01, false)
          )
          then to_json(array(src.sales_avg, ref.sales_avg))
        end as sales_avg
      from _src_prep src
      full outer join _ref_prep ref on
        src.order_date <=> ref.order_date and
        src.category_id <=> ref.category_id and
        src._rank <=> ref._rank
    )
    select * from _result_full
    where
      _exists is not null or
      sales_total is not null or
      sales_avg is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## group and sort
    sql_rendered = Template(TEMPLATE_ACCURACY_SORTED).render(
      src="real_result",
      ref="expected_result",
      group=["order_date", "category_id"],
      sort=["order_counts", "sales_total"],
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          partition by
            order_date,
            category_id
          order by
            order_counts,
            sales_total
        ) as _rank
      from real_result
    ),
    _ref_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          partition by
            order_date,
            category_id
          order by
            order_counts,
            sales_total
        ) as _rank
      from expected_result
    ),
    _result_full as (
      select
        nvl(src.order_date, ref.order_date) as order_date,
        nvl(src.category_id, ref.category_id) as category_id,
        nvl(src._rank, ref._rank) as _rank,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists
      from _src_prep src
      full outer join _ref_prep ref on
        src.order_date <=> ref.order_date and
        src.category_id <=> ref.category_id and
        src._rank <=> ref._rank
    )
    select * from _result_full
    where
      _exists is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## sort, exact and approx
    sql_rendered = Template(TEMPLATE_ACCURACY_SORTED).render(
      src="real_result",
      ref="expected_result",
      sort=["order_counts", "sales_total"],
      exact=["order_month", "order_counts"],
      approx={"sales_total": "1e-12", "sales_avg": "0.01"},
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          order by
            order_counts,
            sales_total
        ) as _rank
      from real_result
    ),
    _ref_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          order by
            order_counts,
            sales_total
        ) as _rank
      from expected_result
    ),
    _result_full as (
      select
        nvl(src._rank, ref._rank) as _rank,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists,
        case
          when not(src.order_month <=> ref.order_month)
          then to_json(array(src.order_month, ref.order_month))
        end as order_month,
        case
          when not(src.order_counts <=> ref.order_counts)
          then to_json(array(src.order_counts, ref.order_counts))
        end as order_counts,
        case
          when not(
            (src.sales_total is null and ref.sales_total is null) or
            nvl(abs(src.sales_total - ref.sales_total) <= 1e-12, false)
          )
          then to_json(array(src.sales_total, ref.sales_total))
        end as sales_total,
        case
          when not(
            (src.sales_avg is null and ref.sales_avg is null) or
            nvl(abs(src.sales_avg - ref.sales_avg) <= 0.01, false)
          )
          then to_json(array(src.sales_avg, ref.sales_avg))
        end as sales_avg
      from _src_prep src
      full outer join _ref_prep ref on
        src._rank <=> ref._rank
    )
    select * from _result_full
    where
      _exists is not null or
      order_month is not null or
      order_counts is not null or
      sales_total is not null or
      sales_avg is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## sort and exact
    sql_rendered = Template(TEMPLATE_ACCURACY_SORTED).render(
      src="real_result",
      ref="expected_result",
      sort=["order_counts", "sales_total"],
      exact=["order_month", "order_counts"],
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          order by
            order_counts,
            sales_total
        ) as _rank
      from real_result
    ),
    _ref_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          order by
            order_counts,
            sales_total
        ) as _rank
      from expected_result
    ),
    _result_full as (
      select
        nvl(src._rank, ref._rank) as _rank,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists,
        case
          when not(src.order_month <=> ref.order_month)
          then to_json(array(src.order_month, ref.order_month))
        end as order_month,
        case
          when not(src.order_counts <=> ref.order_counts)
          then to_json(array(src.order_counts, ref.order_counts))
        end as order_counts
      from _src_prep src
      full outer join _ref_prep ref on
        src._rank <=> ref._rank
    )
    select * from _result_full
    where
      _exists is not null or
      order_month is not null or
      order_counts is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## sort and approx
    sql_rendered = Template(TEMPLATE_ACCURACY_SORTED).render(
      src="real_result",
      ref="expected_result",
      sort=["order_counts", "sales_total"],
      approx={"sales_total": "1e-12", "sales_avg": "0.01"},
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          order by
            order_counts,
            sales_total
        ) as _rank
      from real_result
    ),
    _ref_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          order by
            order_counts,
            sales_total
        ) as _rank
      from expected_result
    ),
    _result_full as (
      select
        nvl(src._rank, ref._rank) as _rank,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists,
        case
          when not(
            (src.sales_total is null and ref.sales_total is null) or
            nvl(abs(src.sales_total - ref.sales_total) <= 1e-12, false)
          )
          then to_json(array(src.sales_total, ref.sales_total))
        end as sales_total,
        case
          when not(
            (src.sales_avg is null and ref.sales_avg is null) or
            nvl(abs(src.sales_avg - ref.sales_avg) <= 0.01, false)
          )
          then to_json(array(src.sales_avg, ref.sales_avg))
        end as sales_avg
      from _src_prep src
      full outer join _ref_prep ref on
        src._rank <=> ref._rank
    )
    select * from _result_full
    where
      _exists is not null or
      sales_total is not null or
      sales_avg is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
    
    ## sort only
    sql_rendered = Template(TEMPLATE_ACCURACY_SORTED).render(
      src="real_result",
      ref="expected_result",
      sort=["order_counts", "sales_total"],
    )
    
    sql_expected = dedent("""
    with _src_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          order by
            order_counts,
            sales_total
        ) as _rank
      from real_result
    ),
    _ref_prep as (
      select 
        *, 
        true as _exists,
        row_number() over(
          order by
            order_counts,
            sales_total
        ) as _rank
      from expected_result
    ),
    _result_full as (
      select
        nvl(src._rank, ref._rank) as _rank,
        case
          when not(src._exists <=> ref._exists)
          then to_json(array(nvl(src._exists, false), nvl(ref._exists, false)))
        end as _exists
      from _src_prep src
      full outer join _ref_prep ref on
        src._rank <=> ref._rank
    )
    select * from _result_full
    where
      _exists is not null;
    """).strip()
    
    assert sql_rendered == sql_expected, f"SQL queries are different"
