import pyspark.sql.functions as f


def with_top_parent_id(df):
    return df \
        .withColumn("top_parent_id", f.when(f.col("parent_id").isNull(), f.col("id"))
                    .otherwise(f.split(f.col("path")[0], '\|')[0])) \
        .withColumnRenamed("id", "sample_id")


def with_measurement_pivot(df):
    return df.groupBy("sample_id") \
        .pivot("measurement_type") \
        .agg(f.first("value"))


def clean_df_names(df):
    cols = df.columns
    new_cols = []
    for col in cols:
        if col != "sample_id":
            new_cols.append('measurement_' + col)
        else:
            new_cols.append(col)
    return df.toDF(*new_cols)


def with_select_query(df):
    cols = df.columns
    return cols + ['experiment_id', 'top_parent_id']


def with_create_query(cols, df):
    new_cols = []
    for col in cols:
        if col.startswith("measurement_"):
            new_cols.append(' ' + col + ' DECIMAL')
        else:
            new_cols.append(' ' + col + ' BIGINT')

    return ",".join(new_cols)


def with_alter_query(cols, df):
    table_cols_len = len(df.columns)
    cols_len = len(cols)
    new_cols = []
    if cols_len - table_cols_len == 0:
        new_cols.append('SET @@session.unique_checks = 0;')
    else:
        new_cols.append('SET @@session.unique_checks = 0; ALTER TABLE IF EXISTS experiment_measurements ')
        for col in cols:
            if col.startswith("measurement_"):
                new_cols.append('ADD COLUMN IF NOT EXISTS ' + col + ' DECIMAL')
            else:
                new_cols.append('ADD COLUMN IF NOT EXISTS ' + col + ' BIGINT')

    return ",".join(new_cols)
