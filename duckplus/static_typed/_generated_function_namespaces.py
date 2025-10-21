    duckdb_function,
    @duckdb_function('arg_max')
    @duckdb_function('arg_max_filter')
    @duckdb_function('arg_max_null')
    @duckdb_function('arg_max_null_filter')
    @duckdb_function('arg_min')
    @duckdb_function('arg_min_filter')
    @duckdb_function('arg_min_null')
    @duckdb_function('arg_min_null_filter')
    @duckdb_function('argmax')
    @duckdb_function('argmax_filter')
    @duckdb_function('argmin')
    @duckdb_function('argmin_filter')
    @duckdb_function('max_by')
    @duckdb_function('max_by_filter')
    @duckdb_function('min_by')
    @duckdb_function('min_by_filter')

    @duckdb_function('bool_and')
    @duckdb_function('bool_and_filter')
    @duckdb_function('bool_or')
    @duckdb_function('bool_or_filter')

    @duckdb_function('any_value')
    @duckdb_function('any_value_filter')
    @duckdb_function('approx_quantile')
    @duckdb_function('approx_quantile_filter')
    @duckdb_function('approx_top_k')
    @duckdb_function('approx_top_k_filter')
    @duckdb_function('arbitrary')
    @duckdb_function('arbitrary_filter')
    @duckdb_function('arg_max')
    @duckdb_function('arg_max_filter')
    @duckdb_function('arg_max_null')
    @duckdb_function('arg_max_null_filter')
    @duckdb_function('arg_min')
    @duckdb_function('arg_min_filter')
    @duckdb_function('arg_min_null')
    @duckdb_function('arg_min_null_filter')
    @duckdb_function('argmax')
    @duckdb_function('argmax_filter')
    @duckdb_function('argmin')
    @duckdb_function('argmin_filter')
    @duckdb_function('array_agg')
    @duckdb_function('array_agg_filter')
    @duckdb_function('avg')
    @duckdb_function('avg_filter')
    @duckdb_function('bit_and')
    @duckdb_function('bit_and_filter')
    @duckdb_function('bit_or')
    @duckdb_function('bit_or_filter')
    @duckdb_function('bit_xor')
    @duckdb_function('bit_xor_filter')
    @duckdb_function('bitstring_agg')
    @duckdb_function('bitstring_agg_filter')
    @duckdb_function('first')
    @duckdb_function('first_filter')
    @duckdb_function('histogram')
    @duckdb_function('histogram_filter')
    @duckdb_function('histogram_exact')
    @duckdb_function('histogram_exact_filter')
    @duckdb_function('last')
    @duckdb_function('last_filter')
    @duckdb_function('list')
    @duckdb_function('list_filter')
    @duckdb_function('max')
    @duckdb_function('max_filter')
    @duckdb_function('max_by')
    @duckdb_function('max_by_filter')
    @duckdb_function('mean')
    @duckdb_function('mean_filter')
    @duckdb_function('median')
    @duckdb_function('median_filter')
    @duckdb_function('min')
    @duckdb_function('min_filter')
    @duckdb_function('min_by')
    @duckdb_function('min_by_filter')
    @duckdb_function('mode')
    @duckdb_function('mode_filter')
    @duckdb_function('quantile')
    @duckdb_function('quantile_filter')
    @duckdb_function('quantile_cont')
    @duckdb_function('quantile_cont_filter')
    @duckdb_function('quantile_disc')
    @duckdb_function('quantile_disc_filter')
    @duckdb_function('sum')
    @duckdb_function('sum_filter')

    @duckdb_function('any_value')
    @duckdb_function('any_value_filter')
    @duckdb_function('approx_count_distinct')
    @duckdb_function('approx_count_distinct_filter')
    @duckdb_function('approx_quantile')
    @duckdb_function('approx_quantile_filter')
    @duckdb_function('arbitrary')
    @duckdb_function('arbitrary_filter')
    @duckdb_function('arg_max')
    @duckdb_function('arg_max_filter')
    @duckdb_function('arg_max_null')
    @duckdb_function('arg_max_null_filter')
    @duckdb_function('arg_min')
    @duckdb_function('arg_min_filter')
    @duckdb_function('arg_min_null')
    @duckdb_function('arg_min_null_filter')
    @duckdb_function('argmax')
    @duckdb_function('argmax_filter')
    @duckdb_function('argmin')
    @duckdb_function('argmin_filter')
    @duckdb_function('avg')
    @duckdb_function('avg_filter')
    @duckdb_function('bit_and')
    @duckdb_function('bit_and_filter')
    @duckdb_function('bit_or')
    @duckdb_function('bit_or_filter')
    @duckdb_function('bit_xor')
    @duckdb_function('bit_xor_filter')
    @duckdb_function('corr')
    @duckdb_function('corr_filter')
    @duckdb_function('count')
    @duckdb_function('count_filter')
    @duckdb_function('count_if')
    @duckdb_function('count_if_filter')
    @duckdb_function('count_star')
    @duckdb_function('count_star_filter')
    @duckdb_function('countif')
    @duckdb_function('countif_filter')
    @duckdb_function('covar_pop')
    @duckdb_function('covar_pop_filter')
    @duckdb_function('covar_samp')
    @duckdb_function('covar_samp_filter')
    @duckdb_function('entropy')
    @duckdb_function('entropy_filter')
    @duckdb_function('favg')
    @duckdb_function('favg_filter')
    @duckdb_function('first')
    @duckdb_function('first_filter')
    @duckdb_function('fsum')
    @duckdb_function('fsum_filter')
    @duckdb_function('kahan_sum')
    @duckdb_function('kahan_sum_filter')
    @duckdb_function('kurtosis')
    @duckdb_function('kurtosis_filter')
    @duckdb_function('kurtosis_pop')
    @duckdb_function('kurtosis_pop_filter')
    @duckdb_function('last')
    @duckdb_function('last_filter')
    @duckdb_function('mad')
    @duckdb_function('mad_filter')
    @duckdb_function('max_by')
    @duckdb_function('max_by_filter')
    @duckdb_function('mean')
    @duckdb_function('mean_filter')
    @duckdb_function('min_by')
    @duckdb_function('min_by_filter')
    @duckdb_function('product')
    @duckdb_function('product_filter')
    @duckdb_function('quantile_cont')
    @duckdb_function('quantile_cont_filter')
    @duckdb_function('regr_avgx')
    @duckdb_function('regr_avgx_filter')
    @duckdb_function('regr_avgy')
    @duckdb_function('regr_avgy_filter')
    @duckdb_function('regr_count')
    @duckdb_function('regr_count_filter')
    @duckdb_function('regr_intercept')
    @duckdb_function('regr_intercept_filter')
    @duckdb_function('regr_r2')
    @duckdb_function('regr_r2_filter')
    @duckdb_function('regr_slope')
    @duckdb_function('regr_slope_filter')
    @duckdb_function('regr_sxx')
    @duckdb_function('regr_sxx_filter')
    @duckdb_function('regr_sxy')
    @duckdb_function('regr_sxy_filter')
    @duckdb_function('regr_syy')
    @duckdb_function('regr_syy_filter')
    @duckdb_function('reservoir_quantile')
    @duckdb_function('reservoir_quantile_filter')
    @duckdb_function('sem')
    @duckdb_function('sem_filter')
    @duckdb_function('skewness')
    @duckdb_function('skewness_filter')
    @duckdb_function('stddev')
    @duckdb_function('stddev_filter')
    @duckdb_function('stddev_pop')
    @duckdb_function('stddev_pop_filter')
    @duckdb_function('stddev_samp')
    @duckdb_function('stddev_samp_filter')
    @duckdb_function('sum')
    @duckdb_function('sum_filter')
    @duckdb_function('sum_no_overflow')
    @duckdb_function('sum_no_overflow_filter')
    @duckdb_function('sumkahan')
    @duckdb_function('sumkahan_filter')
    @duckdb_function('var_pop')
    @duckdb_function('var_pop_filter')
    @duckdb_function('var_samp')
    @duckdb_function('var_samp_filter')
    @duckdb_function('variance')
    @duckdb_function('variance_filter')

    @duckdb_function('arg_max')
    @duckdb_function('arg_max_filter')
    @duckdb_function('arg_max_null')
    @duckdb_function('arg_max_null_filter')
    @duckdb_function('arg_min')
    @duckdb_function('arg_min_filter')
    @duckdb_function('arg_min_null')
    @duckdb_function('arg_min_null_filter')
    @duckdb_function('argmax')
    @duckdb_function('argmax_filter')
    @duckdb_function('argmin')
    @duckdb_function('argmin_filter')
    @duckdb_function('group_concat')
    @duckdb_function('group_concat_filter')
    @duckdb_function('listagg')
    @duckdb_function('listagg_filter')
    @duckdb_function('max_by')
    @duckdb_function('max_by_filter')
    @duckdb_function('min_by')
    @duckdb_function('min_by_filter')
    @duckdb_function('string_agg')
    @duckdb_function('string_agg_filter')

    @duckdb_function('create_sort_key')
    @duckdb_function('encode')
    @duckdb_function('from_base64')
    @duckdb_function('from_binary')
    @duckdb_function('from_hex')
    @duckdb_function('repeat')
    @duckdb_function('unbin')
    @duckdb_function('unhex')

    @duckdb_function('array_contains')
    @duckdb_function('array_has')
    @duckdb_function('array_has_all')
    @duckdb_function('array_has_any')
    @duckdb_function('can_cast_implicitly')
    @duckdb_function('contains')
    @duckdb_function('ends_with')
    _HAS_ANY_COLUMN_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_any_column_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('table', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_any_column_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'table', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_any_column_privilege')
    def has_any_column_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_any_column_privilege``.

        Overloads:
        - pg_catalog.has_any_column_privilege(ANY table, ANY privilege) -> BOOLEAN
        - pg_catalog.has_any_column_privilege(ANY user, ANY table, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_ANY_COLUMN_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _HAS_COLUMN_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_column_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('table', 'column', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_column_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None, None),
                        parameters=('user', 'table', 'column', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_column_privilege')
    def has_column_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_column_privilege``.

        Overloads:
        - pg_catalog.has_column_privilege(ANY table, ANY column, ANY privilege) -> BOOLEAN
        - pg_catalog.has_column_privilege(ANY user, ANY table, ANY column, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_COLUMN_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _HAS_DATABASE_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_database_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('database', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_database_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'database', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_database_privilege')
    def has_database_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_database_privilege``.

        Overloads:
        - pg_catalog.has_database_privilege(ANY database, ANY privilege) -> BOOLEAN
        - pg_catalog.has_database_privilege(ANY user, ANY database, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_DATABASE_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _HAS_FOREIGN_DATA_WRAPPER_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_foreign_data_wrapper_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('fdw', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_foreign_data_wrapper_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'fdw', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_foreign_data_wrapper_privilege')
    def has_foreign_data_wrapper_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_foreign_data_wrapper_privilege``.

        Overloads:
        - pg_catalog.has_foreign_data_wrapper_privilege(ANY fdw, ANY privilege) -> BOOLEAN
        - pg_catalog.has_foreign_data_wrapper_privilege(ANY user, ANY fdw, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_FOREIGN_DATA_WRAPPER_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _HAS_FUNCTION_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_function_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('function', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_function_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'function', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_function_privilege')
    def has_function_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_function_privilege``.

        Overloads:
        - pg_catalog.has_function_privilege(ANY function, ANY privilege) -> BOOLEAN
        - pg_catalog.has_function_privilege(ANY user, ANY function, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_FUNCTION_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _HAS_LANGUAGE_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_language_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('language', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_language_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'language', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_language_privilege')
    def has_language_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_language_privilege``.

        Overloads:
        - pg_catalog.has_language_privilege(ANY language, ANY privilege) -> BOOLEAN
        - pg_catalog.has_language_privilege(ANY user, ANY language, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_LANGUAGE_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _HAS_SCHEMA_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_schema_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('schema', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_schema_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'schema', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_schema_privilege')
    def has_schema_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_schema_privilege``.

        Overloads:
        - pg_catalog.has_schema_privilege(ANY schema, ANY privilege) -> BOOLEAN
        - pg_catalog.has_schema_privilege(ANY user, ANY schema, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_SCHEMA_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _HAS_SEQUENCE_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_sequence_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('sequence', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_sequence_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'sequence', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_sequence_privilege')
    def has_sequence_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_sequence_privilege``.

        Overloads:
        - pg_catalog.has_sequence_privilege(ANY sequence, ANY privilege) -> BOOLEAN
        - pg_catalog.has_sequence_privilege(ANY user, ANY sequence, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_SEQUENCE_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _HAS_SERVER_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_server_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('server', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_server_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'server', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_server_privilege')
    def has_server_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_server_privilege``.

        Overloads:
        - pg_catalog.has_server_privilege(ANY server, ANY privilege) -> BOOLEAN
        - pg_catalog.has_server_privilege(ANY user, ANY server, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_SERVER_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _HAS_TABLE_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_table_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('table', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_table_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'table', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_table_privilege')
    def has_table_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_table_privilege``.

        Overloads:
        - pg_catalog.has_table_privilege(ANY table, ANY privilege) -> BOOLEAN
        - pg_catalog.has_table_privilege(ANY user, ANY table, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_TABLE_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _HAS_TABLESPACE_PRIVILEGE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_tablespace_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('tablespace', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='has_tablespace_privilege',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'tablespace', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('has_tablespace_privilege')
    def has_tablespace_privilege(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``has_tablespace_privilege``.

        Overloads:
        - pg_catalog.has_tablespace_privilege(ANY tablespace, ANY privilege) -> BOOLEAN
        - pg_catalog.has_tablespace_privilege(ANY user, ANY tablespace, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._HAS_TABLESPACE_PRIVILEGE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('ilike_escape')
    @duckdb_function('in_search_path')
    @duckdb_function('is_histogram_other_bin')
    @duckdb_function('isfinite')
    @duckdb_function('isinf')
    @duckdb_function('isnan')
    @duckdb_function('json_contains')
    @duckdb_function('json_exists')
    @duckdb_function('json_valid')
    @duckdb_function('like_escape')
    @duckdb_function('list_contains')
    @duckdb_function('list_has')
    @duckdb_function('list_has_all')
    @duckdb_function('list_has_any')
    @duckdb_function('map_contains')
    _MAP_CONTAINS_ENTRY_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='map_contains_entry',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('map', 'key', 'value'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='contains(map_entries("map"), main.struct_pack("key" := "key", "value" := "value"))',
                    ),
    )
    @duckdb_function('map_contains_entry')
    def map_contains_entry(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``map_contains_entry``.

        Overloads:
        - main.map_contains_entry(ANY map, ANY key, ANY value) -> BOOLEAN
        """
        return call_duckdb_function(
            self._MAP_CONTAINS_ENTRY_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _MAP_CONTAINS_VALUE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='map_contains_value',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('map', 'value'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='contains(map_values("map"), "value")',
                    ),
    )
    @duckdb_function('map_contains_value')
    def map_contains_value(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``map_contains_value``.

        Overloads:
        - main.map_contains_value(ANY map, ANY value) -> BOOLEAN
        """
        return call_duckdb_function(
            self._MAP_CONTAINS_VALUE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('not_ilike_escape')
    @duckdb_function('not_like_escape')
    _PG_COLLATION_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_collation_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('collation_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_collation_is_visible')
    def pg_collation_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_collation_is_visible``.

        Overloads:
        - pg_catalog.pg_collation_is_visible(ANY collation_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_COLLATION_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_CONVERSION_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_conversion_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('conversion_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_conversion_is_visible')
    def pg_conversion_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_conversion_is_visible``.

        Overloads:
        - pg_catalog.pg_conversion_is_visible(ANY conversion_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_CONVERSION_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_FUNCTION_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_function_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('function_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_function_is_visible')
    def pg_function_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_function_is_visible``.

        Overloads:
        - pg_catalog.pg_function_is_visible(ANY function_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_FUNCTION_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_HAS_ROLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_has_role',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None),
                        parameters=('role', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_has_role',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None, None, None),
                        parameters=('user', 'role', 'privilege'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_has_role')
    def pg_has_role(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_has_role``.

        Overloads:
        - pg_catalog.pg_has_role(ANY role, ANY privilege) -> BOOLEAN
        - pg_catalog.pg_has_role(ANY user, ANY role, ANY privilege) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_HAS_ROLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_IS_OTHER_TEMP_SCHEMA_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_is_other_temp_schema',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('schema_id',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('f' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_is_other_temp_schema')
    def pg_is_other_temp_schema(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_is_other_temp_schema``.

        Overloads:
        - pg_catalog.pg_is_other_temp_schema(ANY schema_id) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_IS_OTHER_TEMP_SCHEMA_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_OPCLASS_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_opclass_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('opclass_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_opclass_is_visible')
    def pg_opclass_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_opclass_is_visible``.

        Overloads:
        - pg_catalog.pg_opclass_is_visible(ANY opclass_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_OPCLASS_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_OPERATOR_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_operator_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('operator_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_operator_is_visible')
    def pg_operator_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_operator_is_visible``.

        Overloads:
        - pg_catalog.pg_operator_is_visible(ANY operator_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_OPERATOR_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_OPFAMILY_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_opfamily_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('opclass_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_opfamily_is_visible')
    def pg_opfamily_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_opfamily_is_visible``.

        Overloads:
        - pg_catalog.pg_opfamily_is_visible(ANY opclass_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_OPFAMILY_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_TABLE_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_table_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('table_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_table_is_visible')
    def pg_table_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_table_is_visible``.

        Overloads:
        - pg_catalog.pg_table_is_visible(ANY table_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_TABLE_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_TS_CONFIG_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_ts_config_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('config_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_ts_config_is_visible')
    def pg_ts_config_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_ts_config_is_visible``.

        Overloads:
        - pg_catalog.pg_ts_config_is_visible(ANY config_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_TS_CONFIG_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_TS_DICT_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_ts_dict_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('dict_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_ts_dict_is_visible')
    def pg_ts_dict_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_ts_dict_is_visible``.

        Overloads:
        - pg_catalog.pg_ts_dict_is_visible(ANY dict_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_TS_DICT_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_TS_PARSER_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_ts_parser_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('parser_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_ts_parser_is_visible')
    def pg_ts_parser_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_ts_parser_is_visible``.

        Overloads:
        - pg_catalog.pg_ts_parser_is_visible(ANY parser_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_TS_PARSER_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_TS_TEMPLATE_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_ts_template_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('template_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_ts_template_is_visible')
    def pg_ts_template_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_ts_template_is_visible``.

        Overloads:
        - pg_catalog.pg_ts_template_is_visible(ANY template_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_TS_TEMPLATE_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_TYPE_IS_VISIBLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_type_is_visible',
                        function_type=function_type,
                        return_type=parse_type('BOOLEAN'),
                        parameter_types=(None,),
                        parameters=('type_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CAST('t' AS BOOLEAN)",
                    ),
    )
    @duckdb_function('pg_type_is_visible')
    def pg_type_is_visible(self, *operands: object) -> BooleanExpression:
        """Call DuckDB function ``pg_type_is_visible``.

        Overloads:
        - pg_catalog.pg_type_is_visible(ANY type_oid) -> BOOLEAN
        """
        return call_duckdb_function(
            self._PG_TYPE_IS_VISIBLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('prefix')
    @duckdb_function('regexp_full_match')
    @duckdb_function('regexp_matches')
    @duckdb_function('signbit')
    @duckdb_function('starts_with')
    @duckdb_function('struct_contains')
    @duckdb_function('struct_has')
    @duckdb_function('suffix')
    @duckdb_function(symbols=('!~~',))
    @duckdb_function(symbols=('!~~*',))
    @duckdb_function(symbols=('&&',))
    @duckdb_function(symbols=('<@',))
    @duckdb_function(symbols=('@>',))
    @duckdb_function(symbols=('^@',))
    @duckdb_function(symbols=('~~',))
    @duckdb_function(symbols=('~~*',))
    @duckdb_function(symbols=('~~~',))

        'has_any_column_privilege': 'has_any_column_privilege',
        'has_column_privilege': 'has_column_privilege',
        'has_database_privilege': 'has_database_privilege',
        'has_foreign_data_wrapper_privilege': 'has_foreign_data_wrapper_privilege',
        'has_function_privilege': 'has_function_privilege',
        'has_language_privilege': 'has_language_privilege',
        'has_schema_privilege': 'has_schema_privilege',
        'has_sequence_privilege': 'has_sequence_privilege',
        'has_server_privilege': 'has_server_privilege',
        'has_table_privilege': 'has_table_privilege',
        'has_tablespace_privilege': 'has_tablespace_privilege',
        'map_contains_entry': 'map_contains_entry',
        'map_contains_value': 'map_contains_value',
        'pg_collation_is_visible': 'pg_collation_is_visible',
        'pg_conversion_is_visible': 'pg_conversion_is_visible',
        'pg_function_is_visible': 'pg_function_is_visible',
        'pg_has_role': 'pg_has_role',
        'pg_is_other_temp_schema': 'pg_is_other_temp_schema',
        'pg_opclass_is_visible': 'pg_opclass_is_visible',
        'pg_operator_is_visible': 'pg_operator_is_visible',
        'pg_opfamily_is_visible': 'pg_opfamily_is_visible',
        'pg_table_is_visible': 'pg_table_is_visible',
        'pg_ts_config_is_visible': 'pg_ts_config_is_visible',
        'pg_ts_dict_is_visible': 'pg_ts_dict_is_visible',
        'pg_ts_parser_is_visible': 'pg_ts_parser_is_visible',
        'pg_ts_template_is_visible': 'pg_ts_template_is_visible',
        'pg_type_is_visible': 'pg_type_is_visible',

    @duckdb_function('__internal_compress_string_uhugeint')
    @duckdb_function('__internal_decompress_integral_uhugeint')
    @duckdb_function('abs')
    @duckdb_function('add')
    @duckdb_function('aggregate')
    @duckdb_function('apply')
    @duckdb_function('array_aggr')
    @duckdb_function('array_aggregate')
    _ARRAY_APPEND_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='array_append',
                        function_type=function_type,
                        return_type=parse_type('"NULL"[]'),
                        parameter_types=(None, None),
                        parameters=('arr', 'el'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='list_append(arr, el)',
                    ),
    )
    @duckdb_function('array_append')
    def array_append(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``array_append``.

        Overloads:
        - main.array_append(ANY arr, ANY el) -> "NULL"[]
        """
        return call_duckdb_function(
            self._ARRAY_APPEND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('array_apply')
    @duckdb_function('array_cat')
    @duckdb_function('array_concat')
    @duckdb_function('array_distinct')
    @duckdb_function('array_extract')
    @duckdb_function('array_filter')
    @duckdb_function('array_grade_up')
    _ARRAY_INTERSECT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='array_intersect',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None, None),
                        parameters=('l1', 'l2'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='list_intersect(l1, l2)',
                    ),
    )
    @duckdb_function('array_intersect')
    def array_intersect(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``array_intersect``.

        Overloads:
        - main.array_intersect(ANY l1, ANY l2) -> "NULL"
        """
        return call_duckdb_function(
            self._ARRAY_INTERSECT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _ARRAY_POP_BACK_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='array_pop_back',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('arr',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='arr[:(len(arr) - 1)]',
                    ),
    )
    @duckdb_function('array_pop_back')
    def array_pop_back(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``array_pop_back``.

        Overloads:
        - main.array_pop_back(ANY arr) -> "NULL"
        """
        return call_duckdb_function(
            self._ARRAY_POP_BACK_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _ARRAY_POP_FRONT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='array_pop_front',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('arr',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='arr[2:]',
                    ),
    )
    @duckdb_function('array_pop_front')
    def array_pop_front(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``array_pop_front``.

        Overloads:
        - main.array_pop_front(ANY arr) -> "NULL"
        """
        return call_duckdb_function(
            self._ARRAY_POP_FRONT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _ARRAY_PREPEND_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='array_prepend',
                        function_type=function_type,
                        return_type=parse_type('"NULL"[]'),
                        parameter_types=(None, None),
                        parameters=('el', 'arr'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='list_prepend(el, arr)',
                    ),
    )
    @duckdb_function('array_prepend')
    def array_prepend(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``array_prepend``.

        Overloads:
        - main.array_prepend(ANY el, ANY arr) -> "NULL"[]
        """
        return call_duckdb_function(
            self._ARRAY_PREPEND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _ARRAY_PUSH_BACK_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='array_push_back',
                        function_type=function_type,
                        return_type=parse_type('"NULL"[]'),
                        parameter_types=(None, None),
                        parameters=('arr', 'e'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='list_concat(arr, list_value(e))',
                    ),
    )
    @duckdb_function('array_push_back')
    def array_push_back(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``array_push_back``.

        Overloads:
        - main.array_push_back(ANY arr, ANY e) -> "NULL"[]
        """
        return call_duckdb_function(
            self._ARRAY_PUSH_BACK_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _ARRAY_PUSH_FRONT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='array_push_front',
                        function_type=function_type,
                        return_type=parse_type('"NULL"[]'),
                        parameter_types=(None, None),
                        parameters=('arr', 'e'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='list_concat(list_value(e), arr)',
                    ),
    )
    @duckdb_function('array_push_front')
    def array_push_front(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``array_push_front``.

        Overloads:
        - main.array_push_front(ANY arr, ANY e) -> "NULL"[]
        """
        return call_duckdb_function(
            self._ARRAY_PUSH_FRONT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('array_reduce')
    @duckdb_function('array_resize')
    _ARRAY_REVERSE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='array_reverse',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='list_reverse(l)',
                    ),
    )
    @duckdb_function('array_reverse')
    def array_reverse(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``array_reverse``.

        Overloads:
        - main.array_reverse(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._ARRAY_REVERSE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('array_reverse_sort')
    @duckdb_function('array_select')
    @duckdb_function('array_slice')
    @duckdb_function('array_sort')
    @duckdb_function('array_transform')
    @duckdb_function('array_value')
    @duckdb_function('array_where')
    @duckdb_function('array_zip')
    @duckdb_function('bitstring')
    @duckdb_function('cast_to_type')
    _COL_DESCRIPTION_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='col_description',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None, None),
                        parameters=('table_oid', 'column_number'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='NULL',
                    ),
    )
    @duckdb_function('col_description')
    def col_description(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``col_description``.

        Overloads:
        - pg_catalog.col_description(ANY table_oid, ANY column_number) -> "NULL"
        """
        return call_duckdb_function(
            self._COL_DESCRIPTION_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('combine')
    @duckdb_function('concat')
    @duckdb_function('constant_or_null')
    @duckdb_function('current_date')
    @duckdb_function('current_localtime')
    @duckdb_function('current_localtimestamp')
    @duckdb_function('current_setting')
    @duckdb_function('date_part')
    @duckdb_function('date_trunc')
    @duckdb_function('datepart')
    @duckdb_function('datetrunc')
    @duckdb_function('divide')
    @duckdb_function('element_at')
    @duckdb_function('enum_code')
    @duckdb_function('epoch_ms')
    @duckdb_function('equi_width_bins')
    @duckdb_function('error')
    @duckdb_function('filter')
    @duckdb_function('finalize')
    @duckdb_function('flatten')
    @duckdb_function('from_json')
    @duckdb_function('from_json_strict')
    @duckdb_function('generate_series')
    _GENERATE_SUBSCRIPTS_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='generate_subscripts',
                        function_type=function_type,
                        return_type=None,
                        parameter_types=(None, None),
                        parameters=('arr', 'dim'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='unnest(generate_series(1, array_length(arr, dim)))',
                    ),
    )
    @duckdb_function('generate_subscripts')
    def generate_subscripts(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``generate_subscripts``.

        Overloads:
        - main.generate_subscripts(ANY arr, ANY dim) -> ANY
        """
        return call_duckdb_function(
            self._GENERATE_SUBSCRIPTS_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('get_current_time')
    @duckdb_function('get_current_timestamp')
    @duckdb_function('getvariable')
    @duckdb_function('grade_up')
    @duckdb_function('greatest')
    _INET_CLIENT_ADDR_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='inet_client_addr',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='NULL',
                    ),
    )
    @duckdb_function('inet_client_addr')
    def inet_client_addr(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``inet_client_addr``.

        Overloads:
        - pg_catalog.inet_client_addr() -> "NULL"
        """
        return call_duckdb_function(
            self._INET_CLIENT_ADDR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _INET_CLIENT_PORT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='inet_client_port',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='NULL',
                    ),
    )
    @duckdb_function('inet_client_port')
    def inet_client_port(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``inet_client_port``.

        Overloads:
        - pg_catalog.inet_client_port() -> "NULL"
        """
        return call_duckdb_function(
            self._INET_CLIENT_PORT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _INET_SERVER_ADDR_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='inet_server_addr',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='NULL',
                    ),
    )
    @duckdb_function('inet_server_addr')
    def inet_server_addr(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``inet_server_addr``.

        Overloads:
        - pg_catalog.inet_server_addr() -> "NULL"
        """
        return call_duckdb_function(
            self._INET_SERVER_ADDR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _INET_SERVER_PORT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='inet_server_port',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='NULL',
                    ),
    )
    @duckdb_function('inet_server_port')
    def inet_server_port(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``inet_server_port``.

        Overloads:
        - pg_catalog.inet_server_port() -> "NULL"
        """
        return call_duckdb_function(
            self._INET_SERVER_PORT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('json_transform')
    @duckdb_function('json_transform_strict')
    @duckdb_function('last_day')
    @duckdb_function('least')
    @duckdb_function('list_aggr')
    @duckdb_function('list_aggregate')
    _LIST_ANY_VALUE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_any_value',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'any_value')",
                    ),
    )
    @duckdb_function('list_any_value')
    def list_any_value(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_any_value``.

        Overloads:
        - main.list_any_value(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_ANY_VALUE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_APPEND_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_append',
                        function_type=function_type,
                        return_type=parse_type('"NULL"[]'),
                        parameter_types=(None, None),
                        parameters=('l', 'e'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='list_concat(l, list_value(e))',
                    ),
    )
    @duckdb_function('list_append')
    def list_append(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_append``.

        Overloads:
        - main.list_append(ANY l, ANY e) -> "NULL"[]
        """
        return call_duckdb_function(
            self._LIST_APPEND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_apply')
    _LIST_APPROX_COUNT_DISTINCT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_approx_count_distinct',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'approx_count_distinct')",
                    ),
    )
    @duckdb_function('list_approx_count_distinct')
    def list_approx_count_distinct(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_approx_count_distinct``.

        Overloads:
        - main.list_approx_count_distinct(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_APPROX_COUNT_DISTINCT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_AVG_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_avg',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'avg')",
                    ),
    )
    @duckdb_function('list_avg')
    def list_avg(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_avg``.

        Overloads:
        - main.list_avg(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_AVG_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_BIT_AND_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_bit_and',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'bit_and')",
                    ),
    )
    @duckdb_function('list_bit_and')
    def list_bit_and(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_bit_and``.

        Overloads:
        - main.list_bit_and(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_BIT_AND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_BIT_OR_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_bit_or',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'bit_or')",
                    ),
    )
    @duckdb_function('list_bit_or')
    def list_bit_or(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_bit_or``.

        Overloads:
        - main.list_bit_or(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_BIT_OR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_BIT_XOR_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_bit_xor',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'bit_xor')",
                    ),
    )
    @duckdb_function('list_bit_xor')
    def list_bit_xor(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_bit_xor``.

        Overloads:
        - main.list_bit_xor(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_BIT_XOR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_BOOL_AND_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_bool_and',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'bool_and')",
                    ),
    )
    @duckdb_function('list_bool_and')
    def list_bool_and(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_bool_and``.

        Overloads:
        - main.list_bool_and(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_BOOL_AND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_BOOL_OR_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_bool_or',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'bool_or')",
                    ),
    )
    @duckdb_function('list_bool_or')
    def list_bool_or(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_bool_or``.

        Overloads:
        - main.list_bool_or(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_BOOL_OR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_cat')
    @duckdb_function('list_concat')
    _LIST_COUNT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_count',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'count')",
                    ),
    )
    @duckdb_function('list_count')
    def list_count(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_count``.

        Overloads:
        - main.list_count(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_COUNT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_distinct')
    @duckdb_function('list_element')
    _LIST_ENTROPY_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_entropy',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'entropy')",
                    ),
    )
    @duckdb_function('list_entropy')
    def list_entropy(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_entropy``.

        Overloads:
        - main.list_entropy(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_ENTROPY_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_extract')
    @duckdb_function('list_filter')
    _LIST_FIRST_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_first',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'first')",
                    ),
    )
    @duckdb_function('list_first')
    def list_first(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_first``.

        Overloads:
        - main.list_first(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_FIRST_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_grade_up')
    _LIST_HISTOGRAM_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_histogram',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'histogram')",
                    ),
    )
    @duckdb_function('list_histogram')
    def list_histogram(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_histogram``.

        Overloads:
        - main.list_histogram(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_HISTOGRAM_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_INTERSECT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_intersect',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None, None),
                        parameters=('l1', 'l2'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='list_filter(list_distinct(l1), (lambda variable_intersect: list_contains(l2, variable_intersect)))',
                    ),
    )
    @duckdb_function('list_intersect')
    def list_intersect(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_intersect``.

        Overloads:
        - main.list_intersect(ANY l1, ANY l2) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_INTERSECT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_KURTOSIS_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_kurtosis',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'kurtosis')",
                    ),
    )
    @duckdb_function('list_kurtosis')
    def list_kurtosis(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_kurtosis``.

        Overloads:
        - main.list_kurtosis(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_KURTOSIS_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_KURTOSIS_POP_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_kurtosis_pop',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'kurtosis_pop')",
                    ),
    )
    @duckdb_function('list_kurtosis_pop')
    def list_kurtosis_pop(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_kurtosis_pop``.

        Overloads:
        - main.list_kurtosis_pop(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_KURTOSIS_POP_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_LAST_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_last',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'last')",
                    ),
    )
    @duckdb_function('list_last')
    def list_last(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_last``.

        Overloads:
        - main.list_last(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_LAST_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_MAD_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_mad',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'mad')",
                    ),
    )
    @duckdb_function('list_mad')
    def list_mad(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_mad``.

        Overloads:
        - main.list_mad(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_MAD_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_MAX_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_max',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'max')",
                    ),
    )
    @duckdb_function('list_max')
    def list_max(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_max``.

        Overloads:
        - main.list_max(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_MAX_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_MEDIAN_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_median',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'median')",
                    ),
    )
    @duckdb_function('list_median')
    def list_median(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_median``.

        Overloads:
        - main.list_median(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_MEDIAN_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_MIN_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_min',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'min')",
                    ),
    )
    @duckdb_function('list_min')
    def list_min(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_min``.

        Overloads:
        - main.list_min(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_MIN_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_MODE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_mode',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'mode')",
                    ),
    )
    @duckdb_function('list_mode')
    def list_mode(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_mode``.

        Overloads:
        - main.list_mode(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_MODE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_pack')
    _LIST_PREPEND_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_prepend',
                        function_type=function_type,
                        return_type=parse_type('"NULL"[]'),
                        parameter_types=(None, None),
                        parameters=('e', 'l'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='list_concat(list_value(e), l)',
                    ),
    )
    @duckdb_function('list_prepend')
    def list_prepend(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_prepend``.

        Overloads:
        - main.list_prepend(ANY e, ANY l) -> "NULL"[]
        """
        return call_duckdb_function(
            self._LIST_PREPEND_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_PRODUCT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_product',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'product')",
                    ),
    )
    @duckdb_function('list_product')
    def list_product(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_product``.

        Overloads:
        - main.list_product(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_PRODUCT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_reduce')
    @duckdb_function('list_resize')
    _LIST_REVERSE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_reverse',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='l[:-:-1]',
                    ),
    )
    @duckdb_function('list_reverse')
    def list_reverse(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_reverse``.

        Overloads:
        - main.list_reverse(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_REVERSE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_reverse_sort')
    @duckdb_function('list_select')
    _LIST_SEM_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_sem',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'sem')",
                    ),
    )
    @duckdb_function('list_sem')
    def list_sem(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_sem``.

        Overloads:
        - main.list_sem(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_SEM_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_SKEWNESS_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_skewness',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'skewness')",
                    ),
    )
    @duckdb_function('list_skewness')
    def list_skewness(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_skewness``.

        Overloads:
        - main.list_skewness(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_SKEWNESS_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_slice')
    @duckdb_function('list_sort')
    _LIST_STDDEV_POP_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_stddev_pop',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'stddev_pop')",
                    ),
    )
    @duckdb_function('list_stddev_pop')
    def list_stddev_pop(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_stddev_pop``.

        Overloads:
        - main.list_stddev_pop(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_STDDEV_POP_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_STDDEV_SAMP_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_stddev_samp',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'stddev_samp')",
                    ),
    )
    @duckdb_function('list_stddev_samp')
    def list_stddev_samp(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_stddev_samp``.

        Overloads:
        - main.list_stddev_samp(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_STDDEV_SAMP_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_STRING_AGG_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_string_agg',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'string_agg')",
                    ),
    )
    @duckdb_function('list_string_agg')
    def list_string_agg(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_string_agg``.

        Overloads:
        - main.list_string_agg(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_STRING_AGG_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_SUM_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_sum',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'sum')",
                    ),
    )
    @duckdb_function('list_sum')
    def list_sum(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_sum``.

        Overloads:
        - main.list_sum(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_SUM_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_transform')
    @duckdb_function('list_value')
    _LIST_VAR_POP_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_var_pop',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'var_pop')",
                    ),
    )
    @duckdb_function('list_var_pop')
    def list_var_pop(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_var_pop``.

        Overloads:
        - main.list_var_pop(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_VAR_POP_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _LIST_VAR_SAMP_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='list_var_samp',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None,),
                        parameters=('l',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(l, 'var_samp')",
                    ),
    )
    @duckdb_function('list_var_samp')
    def list_var_samp(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``list_var_samp``.

        Overloads:
        - main.list_var_samp(ANY l) -> "NULL"
        """
        return call_duckdb_function(
            self._LIST_VAR_SAMP_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('list_where')
    @duckdb_function('list_zip')
    @duckdb_function('make_date')
    @duckdb_function('make_time')
    @duckdb_function('make_timestamp')
    @duckdb_function('make_timestamp_ms')
    @duckdb_function('make_timestamp_ns')
    @duckdb_function('make_timestamptz')
    @duckdb_function('map')
    @duckdb_function('map_concat')
    @duckdb_function('map_entries')
    @duckdb_function('map_extract')
    @duckdb_function('map_extract_value')
    @duckdb_function('map_from_entries')
    @duckdb_function('map_keys')
    @duckdb_function('map_values')
    @duckdb_function('md5_number')
    @duckdb_function('mod')
    @duckdb_function('multiply')
    @duckdb_function('now')
    _NULLIF_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='nullif',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None, None),
                        parameters=('a', 'b'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='CASE  WHEN ((a = b)) THEN (NULL) ELSE a END',
                    ),
    )
    @duckdb_function('nullif')
    def nullif(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``nullif``.

        Overloads:
        - main.nullif(ANY a, ANY b) -> "NULL"
        """
        return call_duckdb_function(
            self._NULLIF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _OBJ_DESCRIPTION_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='obj_description',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None, None),
                        parameters=('object_oid', 'catalog_name'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='NULL',
                    ),
    )
    @duckdb_function('obj_description')
    def obj_description(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``obj_description``.

        Overloads:
        - pg_catalog.obj_description(ANY object_oid, ANY catalog_name) -> "NULL"
        """
        return call_duckdb_function(
            self._OBJ_DESCRIPTION_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('parse_duckdb_log_message')
    _PG_CONF_LOAD_TIME_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_conf_load_time',
                        function_type=function_type,
                        return_type=parse_type('TIMESTAMP WITH TIME ZONE'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='current_timestamp',
                    ),
    )
    @duckdb_function('pg_conf_load_time')
    def pg_conf_load_time(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``pg_conf_load_time``.

        Overloads:
        - pg_catalog.pg_conf_load_time() -> TIMESTAMP WITH TIME ZONE
        """
        return call_duckdb_function(
            self._PG_CONF_LOAD_TIME_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_GET_EXPR_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_get_expr',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None, None),
                        parameters=('pg_node_tree', 'relation_oid'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='pg_node_tree',
                    ),
    )
    @duckdb_function('pg_get_expr')
    def pg_get_expr(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``pg_get_expr``.

        Overloads:
        - pg_catalog.pg_get_expr(ANY pg_node_tree, ANY relation_oid) -> "NULL"
        """
        return call_duckdb_function(
            self._PG_GET_EXPR_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_POSTMASTER_START_TIME_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_postmaster_start_time',
                        function_type=function_type,
                        return_type=parse_type('TIMESTAMP WITH TIME ZONE'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='current_timestamp',
                    ),
    )
    @duckdb_function('pg_postmaster_start_time')
    def pg_postmaster_start_time(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``pg_postmaster_start_time``.

        Overloads:
        - pg_catalog.pg_postmaster_start_time() -> TIMESTAMP WITH TIME ZONE
        """
        return call_duckdb_function(
            self._PG_POSTMASTER_START_TIME_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('range')
    @duckdb_function('reduce')
    _REGEXP_SPLIT_TO_TABLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='regexp_split_to_table',
                        function_type=function_type,
                        return_type=None,
                        parameter_types=(None, None),
                        parameters=('text', 'pattern'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='unnest(string_split_regex("text", pattern))',
                    ),
    )
    @duckdb_function('regexp_split_to_table')
    def regexp_split_to_table(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``regexp_split_to_table``.

        Overloads:
        - main.regexp_split_to_table(ANY text, ANY pattern) -> ANY
        """
        return call_duckdb_function(
            self._REGEXP_SPLIT_TO_TABLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('remap_struct')
    @duckdb_function('repeat')
    @duckdb_function('replace_type')
    @duckdb_function('row')
    @duckdb_function('set_bit')
    @duckdb_function('setseed')
    _SHOBJ_DESCRIPTION_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='shobj_description',
                        function_type=function_type,
                        return_type=parse_type('"NULL"'),
                        parameter_types=(None, None),
                        parameters=('object_oid', 'catalog_name'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='NULL',
                    ),
    )
    @duckdb_function('shobj_description')
    def shobj_description(self, *operands: object) -> TypedExpression:
        """Call DuckDB function ``shobj_description``.

        Overloads:
        - pg_catalog.shobj_description(ANY object_oid, ANY catalog_name) -> "NULL"
        """
        return call_duckdb_function(
            self._SHOBJ_DESCRIPTION_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('strptime')
    @duckdb_function('struct_concat')
    @duckdb_function('struct_extract')
    @duckdb_function('struct_extract_at')
    @duckdb_function('struct_insert')
    @duckdb_function('struct_pack')
    @duckdb_function('struct_update')
    @duckdb_function('subtract')
    @duckdb_function('time_bucket')
    @duckdb_function('timezone')
    @duckdb_function('to_timestamp')
    @duckdb_function('today')
    @duckdb_function('transaction_timestamp')
    @duckdb_function('trunc')
    @duckdb_function('try_strptime')
    @duckdb_function('union_extract')
    @duckdb_function('union_tag')
    @duckdb_function('union_value')
    @duckdb_function('unpivot_list')
    @duckdb_function('uuid_extract_timestamp')
    @duckdb_function('variant_extract')
    @duckdb_function('write_log')
    @duckdb_function('xor')
    @duckdb_function(symbols=('%',))
    @duckdb_function(symbols=('&',))
    @duckdb_function(symbols=('*',))
    @duckdb_function(symbols=('+',))
    @duckdb_function(symbols=('-',))
    @duckdb_function(symbols=('//',))
    @duckdb_function(symbols=('<<',))
    @duckdb_function(symbols=('>>',))
    @duckdb_function(symbols=('@',))
    @duckdb_function(symbols=('|',))
    @duckdb_function(symbols=('||',))
    @duckdb_function(symbols=('~',))

        'array_append': 'array_append',
        'array_intersect': 'array_intersect',
        'array_pop_back': 'array_pop_back',
        'array_pop_front': 'array_pop_front',
        'array_prepend': 'array_prepend',
        'array_push_back': 'array_push_back',
        'array_push_front': 'array_push_front',
        'array_reverse': 'array_reverse',
        'col_description': 'col_description',
        'generate_subscripts': 'generate_subscripts',
        'inet_client_addr': 'inet_client_addr',
        'inet_client_port': 'inet_client_port',
        'inet_server_addr': 'inet_server_addr',
        'inet_server_port': 'inet_server_port',
        'list_any_value': 'list_any_value',
        'list_append': 'list_append',
        'list_approx_count_distinct': 'list_approx_count_distinct',
        'list_avg': 'list_avg',
        'list_bit_and': 'list_bit_and',
        'list_bit_or': 'list_bit_or',
        'list_bit_xor': 'list_bit_xor',
        'list_bool_and': 'list_bool_and',
        'list_bool_or': 'list_bool_or',
        'list_count': 'list_count',
        'list_entropy': 'list_entropy',
        'list_first': 'list_first',
        'list_histogram': 'list_histogram',
        'list_intersect': 'list_intersect',
        'list_kurtosis': 'list_kurtosis',
        'list_kurtosis_pop': 'list_kurtosis_pop',
        'list_last': 'list_last',
        'list_mad': 'list_mad',
        'list_max': 'list_max',
        'list_median': 'list_median',
        'list_min': 'list_min',
        'list_mode': 'list_mode',
        'list_prepend': 'list_prepend',
        'list_product': 'list_product',
        'list_reverse': 'list_reverse',
        'list_sem': 'list_sem',
        'list_skewness': 'list_skewness',
        'list_stddev_pop': 'list_stddev_pop',
        'list_stddev_samp': 'list_stddev_samp',
        'list_string_agg': 'list_string_agg',
        'list_sum': 'list_sum',
        'list_var_pop': 'list_var_pop',
        'list_var_samp': 'list_var_samp',
        'nullif': 'nullif',
        'obj_description': 'obj_description',
        'pg_conf_load_time': 'pg_conf_load_time',
        'pg_get_expr': 'pg_get_expr',
        'pg_postmaster_start_time': 'pg_postmaster_start_time',
        'regexp_split_to_table': 'regexp_split_to_table',
        'shobj_description': 'shobj_description',

    @duckdb_function('__internal_compress_integral_ubigint')
    @duckdb_function('__internal_compress_integral_uinteger')
    @duckdb_function('__internal_compress_integral_usmallint')
    @duckdb_function('__internal_compress_integral_utinyint')
    @duckdb_function('__internal_compress_string_hugeint')
    @duckdb_function('__internal_compress_string_ubigint')
    @duckdb_function('__internal_compress_string_uinteger')
    @duckdb_function('__internal_compress_string_usmallint')
    @duckdb_function('__internal_compress_string_utinyint')
    @duckdb_function('__internal_decompress_integral_bigint')
    @duckdb_function('__internal_decompress_integral_hugeint')
    @duckdb_function('__internal_decompress_integral_integer')
    @duckdb_function('__internal_decompress_integral_smallint')
    @duckdb_function('__internal_decompress_integral_ubigint')
    @duckdb_function('__internal_decompress_integral_uinteger')
    @duckdb_function('__internal_decompress_integral_usmallint')
    @duckdb_function('abs')
    @duckdb_function('acos')
    @duckdb_function('acosh')
    @duckdb_function('add')
    @duckdb_function('age')
    @duckdb_function('array_cosine_distance')
    @duckdb_function('array_cosine_similarity')
    @duckdb_function('array_cross_product')
    @duckdb_function('array_distance')
    @duckdb_function('array_dot_product')
    @duckdb_function('array_indexof')
    @duckdb_function('array_inner_product')
    @duckdb_function('array_length')
    @duckdb_function('array_negative_dot_product')
    @duckdb_function('array_negative_inner_product')
    @duckdb_function('array_position')
    @duckdb_function('array_unique')
    @duckdb_function('ascii')
    @duckdb_function('asin')
    @duckdb_function('asinh')
    @duckdb_function('atan')
    @duckdb_function('atan2')
    @duckdb_function('atanh')
    @duckdb_function('bit_count')
    @duckdb_function('bit_length')
    @duckdb_function('bit_position')
    @duckdb_function('cardinality')
    @duckdb_function('cbrt')
    @duckdb_function('ceil')
    @duckdb_function('ceiling')
    @duckdb_function('century')
    @duckdb_function('char_length')
    @duckdb_function('character_length')
    @duckdb_function('cos')
    @duckdb_function('cosh')
    @duckdb_function('cot')
    @duckdb_function('current_connection_id')
    @duckdb_function('current_query_id')
    @duckdb_function('current_transaction_id')
    @duckdb_function('currval')
    @duckdb_function('damerau_levenshtein')
    _DATE_ADD_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='date_add',
                        function_type=function_type,
                        return_type=parse_type('BIGINT'),
                        parameter_types=(None, None),
                        parameters=('date', 'interval'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='(date + "interval")',
                    ),
    )
    @duckdb_function('date_add')
    def date_add(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``date_add``.

        Overloads:
        - main.date_add(ANY date, ANY interval) -> BIGINT
        """
        return call_duckdb_function(
            self._DATE_ADD_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('date_diff')
    @duckdb_function('date_part')
    @duckdb_function('date_sub')
    @duckdb_function('date_trunc')
    @duckdb_function('datediff')
    @duckdb_function('datepart')
    @duckdb_function('datesub')
    @duckdb_function('datetrunc')
    @duckdb_function('day')
    @duckdb_function('dayofmonth')
    @duckdb_function('dayofweek')
    @duckdb_function('dayofyear')
    @duckdb_function('decade')
    @duckdb_function('degrees')
    @duckdb_function('divide')
    @duckdb_function('editdist3')
    @duckdb_function('epoch')
    @duckdb_function('epoch_ms')
    @duckdb_function('epoch_ns')
    @duckdb_function('epoch_us')
    @duckdb_function('era')
    @duckdb_function('even')
    @duckdb_function('exp')
    @duckdb_function('factorial')
    _FDIV_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='fdiv',
                        function_type=function_type,
                        return_type=parse_type('DOUBLE'),
                        parameter_types=(None, None),
                        parameters=('x', 'y'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='floor((x / y))',
                    ),
    )
    @duckdb_function('fdiv')
    def fdiv(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``fdiv``.

        Overloads:
        - main.fdiv(ANY x, ANY y) -> DOUBLE
        """
        return call_duckdb_function(
            self._FDIV_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('floor')
    _FMOD_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='fmod',
                        function_type=function_type,
                        return_type=parse_type('DOUBLE'),
                        parameter_types=(None, None),
                        parameters=('x', 'y'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='(x - (y * floor((x / y))))',
                    ),
    )
    @duckdb_function('fmod')
    def fmod(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``fmod``.

        Overloads:
        - main.fmod(ANY x, ANY y) -> DOUBLE
        """
        return call_duckdb_function(
            self._FMOD_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('gamma')
    @duckdb_function('gcd')
    @duckdb_function('generate_series')
    _GEOMEAN_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='geomean',
                        function_type=function_type,
                        return_type=parse_type('DOUBLE'),
                        parameter_types=(None,),
                        parameters=('x',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='exp(avg(ln(x)))',
                    ),
    )
    @duckdb_function('geomean')
    def geomean(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``geomean``.

        Overloads:
        - main.geomean(ANY x) -> DOUBLE
        """
        return call_duckdb_function(
            self._GEOMEAN_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _GEOMETRIC_MEAN_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='geometric_mean',
                        function_type=function_type,
                        return_type=parse_type('DOUBLE'),
                        parameter_types=(None,),
                        parameters=('x',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='geomean(x)',
                    ),
    )
    @duckdb_function('geometric_mean')
    def geometric_mean(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``geometric_mean``.

        Overloads:
        - main.geometric_mean(ANY x) -> DOUBLE
        """
        return call_duckdb_function(
            self._GEOMETRIC_MEAN_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('get_bit')
    _GET_BLOCK_SIZE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='get_block_size',
                        function_type=function_type,
                        return_type=parse_type('BIGINT'),
                        parameter_types=(None,),
                        parameters=('db_name',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='(SELECT block_size FROM pragma_database_size() WHERE (database_name = db_name))',
                    ),
    )
    @duckdb_function('get_block_size')
    def get_block_size(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``get_block_size``.

        Overloads:
        - main.get_block_size(ANY db_name) -> BIGINT
        """
        return call_duckdb_function(
            self._GET_BLOCK_SIZE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('greatest_common_divisor')
    @duckdb_function('hamming')
    @duckdb_function('hash')
    @duckdb_function('hour')
    @duckdb_function('instr')
    @duckdb_function('isodow')
    @duckdb_function('isoyear')
    @duckdb_function('jaccard')
    @duckdb_function('jaro_similarity')
    @duckdb_function('jaro_winkler_similarity')
    @duckdb_function('json_array_length')
    @duckdb_function('julian')
    @duckdb_function('lcm')
    @duckdb_function('least_common_multiple')
    @duckdb_function('len')
    @duckdb_function('length')
    @duckdb_function('length_grapheme')
    @duckdb_function('levenshtein')
    @duckdb_function('lgamma')
    @duckdb_function('list_cosine_distance')
    @duckdb_function('list_cosine_similarity')
    @duckdb_function('list_distance')
    @duckdb_function('list_dot_product')
    @duckdb_function('list_indexof')
    @duckdb_function('list_inner_product')
    @duckdb_function('list_negative_dot_product')
    @duckdb_function('list_negative_inner_product')
    @duckdb_function('list_position')
    @duckdb_function('list_unique')
    @duckdb_function('ln')
    @duckdb_function('log')
    @duckdb_function('log10')
    @duckdb_function('log2')
    _MAP_TO_PG_OID_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='map_to_pg_oid',
                        function_type=function_type,
                        return_type=parse_type('INTEGER'),
                        parameter_types=(None,),
                        parameters=('type_name',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CASE  WHEN ((type_name = 'bool')) THEN (16) WHEN ((type_name = 'int16')) THEN (21) WHEN ((type_name = 'int')) THEN (23) WHEN ((type_name = 'bigint')) THEN (20) WHEN ((type_name = 'date')) THEN (1082) WHEN ((type_name = 'time')) THEN (1083) WHEN ((type_name = 'datetime')) THEN (1114) WHEN ((type_name = 'dec')) THEN (1700) WHEN ((type_name = 'float')) THEN (700) WHEN ((type_name = 'double')) THEN (701) WHEN ((type_name = 'bpchar')) THEN (1043) WHEN ((type_name = 'binary')) THEN (17) WHEN ((type_name = 'interval')) THEN (1186) WHEN ((type_name = 'timestamptz')) THEN (1184) WHEN ((type_name = 'timetz')) THEN (1266) WHEN ((type_name = 'bit')) THEN (1560) WHEN ((type_name = 'guid')) THEN (2950) ELSE NULL END",
                    ),
    )
    @duckdb_function('map_to_pg_oid')
    def map_to_pg_oid(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``map_to_pg_oid``.

        Overloads:
        - pg_catalog.map_to_pg_oid(ANY type_name) -> INTEGER
        """
        return call_duckdb_function(
            self._MAP_TO_PG_OID_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _MD5_NUMBER_LOWER_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='md5_number_lower',
                        function_type=function_type,
                        return_type=parse_type('UBIGINT'),
                        parameter_types=(None,),
                        parameters=('param',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='CAST(CAST(CAST(CAST(md5_number(param) AS BIT) AS VARCHAR)[:64] AS BIT) AS UBIGINT)',
                    ),
    )
    @duckdb_function('md5_number_lower')
    def md5_number_lower(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``md5_number_lower``.

        Overloads:
        - main.md5_number_lower(ANY param) -> UBIGINT
        """
        return call_duckdb_function(
            self._MD5_NUMBER_LOWER_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _MD5_NUMBER_UPPER_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='md5_number_upper',
                        function_type=function_type,
                        return_type=parse_type('UBIGINT'),
                        parameter_types=(None,),
                        parameters=('param',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='CAST(CAST(CAST(CAST(md5_number(param) AS BIT) AS VARCHAR)[65:] AS BIT) AS UBIGINT)',
                    ),
    )
    @duckdb_function('md5_number_upper')
    def md5_number_upper(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``md5_number_upper``.

        Overloads:
        - main.md5_number_upper(ANY param) -> UBIGINT
        """
        return call_duckdb_function(
            self._MD5_NUMBER_UPPER_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('microsecond')
    @duckdb_function('millennium')
    @duckdb_function('millisecond')
    @duckdb_function('minute')
    @duckdb_function('mismatches')
    @duckdb_function('mod')
    @duckdb_function('month')
    @duckdb_function('multiply')
    @duckdb_function('nanosecond')
    @duckdb_function('nextafter')
    @duckdb_function('nextval')
    @duckdb_function('normalized_interval')
    @duckdb_function('octet_length')
    @duckdb_function('ord')
    _PG_MY_TEMP_SCHEMA_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_my_temp_schema',
                        function_type=function_type,
                        return_type=parse_type('INTEGER'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='0',
                    ),
    )
    @duckdb_function('pg_my_temp_schema')
    def pg_my_temp_schema(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``pg_my_temp_schema``.

        Overloads:
        - pg_catalog.pg_my_temp_schema() -> INTEGER
        """
        return call_duckdb_function(
            self._PG_MY_TEMP_SCHEMA_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('pi')
    @duckdb_function('position')
    @duckdb_function('pow')
    @duckdb_function('power')
    @duckdb_function('quarter')
    @duckdb_function('radians')
    @duckdb_function('random')
    @duckdb_function('range')
    @duckdb_function('round')
    _ROUND_EVEN_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='round_even',
                        function_type=function_type,
                        return_type=parse_type('DOUBLE'),
                        parameter_types=(None, None),
                        parameters=('x', 'n'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='CASE  WHEN ((((abs(x) * power(10, (n + 1))) % 10) = 5)) THEN ((round((x / 2), n) * 2)) ELSE round(x, n) END',
                    ),
    )
    @duckdb_function('round_even')
    def round_even(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``round_even``.

        Overloads:
        - main.round_even(ANY x, ANY n) -> DOUBLE
        """
        return call_duckdb_function(
            self._ROUND_EVEN_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _ROUNDBANKERS_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='roundbankers',
                        function_type=function_type,
                        return_type=parse_type('DOUBLE'),
                        parameter_types=(None, None),
                        parameters=('x', 'n'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='round_even(x, n)',
                    ),
    )
    @duckdb_function('roundbankers')
    def roundbankers(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``roundbankers``.

        Overloads:
        - main.roundbankers(ANY x, ANY n) -> DOUBLE
        """
        return call_duckdb_function(
            self._ROUNDBANKERS_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('second')
    @duckdb_function('sign')
    @duckdb_function('sin')
    @duckdb_function('sinh')
    @duckdb_function('sqrt')
    @duckdb_function('strlen')
    @duckdb_function('strpos')
    @duckdb_function('struct_indexof')
    @duckdb_function('struct_position')
    @duckdb_function('subtract')
    @duckdb_function('tan')
    @duckdb_function('tanh')
    @duckdb_function('timetz_byte_comparable')
    @duckdb_function('timezone')
    @duckdb_function('timezone_hour')
    @duckdb_function('timezone_minute')
    @duckdb_function('to_centuries')
    @duckdb_function('to_days')
    @duckdb_function('to_decades')
    @duckdb_function('to_hours')
    @duckdb_function('to_microseconds')
    @duckdb_function('to_millennia')
    @duckdb_function('to_milliseconds')
    @duckdb_function('to_minutes')
    @duckdb_function('to_months')
    @duckdb_function('to_quarters')
    @duckdb_function('to_seconds')
    @duckdb_function('to_weeks')
    @duckdb_function('to_years')
    @duckdb_function('trunc')
    @duckdb_function('txid_current')
    @duckdb_function('unicode')
    @duckdb_function('uuid_extract_version')
    _WAVG_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='wavg',
                        function_type=function_type,
                        return_type=parse_type('DOUBLE'),
                        parameter_types=(None, None),
                        parameters=('value', 'weight'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='weighted_avg("value", weight)',
                    ),
    )
    @duckdb_function('wavg')
    def wavg(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``wavg``.

        Overloads:
        - main.wavg(ANY value, ANY weight) -> DOUBLE
        """
        return call_duckdb_function(
            self._WAVG_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('week')
    @duckdb_function('weekday')
    @duckdb_function('weekofyear')
    _WEIGHTED_AVG_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='weighted_avg',
                        function_type=function_type,
                        return_type=parse_type('DOUBLE'),
                        parameter_types=(None, None),
                        parameters=('value', 'weight'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='(sum(("value" * weight)) / sum(CASE  WHEN (("value" IS NOT NULL)) THEN (weight) ELSE 0 END))',
                    ),
    )
    @duckdb_function('weighted_avg')
    def weighted_avg(self, *operands: object) -> NumericExpression:
        """Call DuckDB function ``weighted_avg``.

        Overloads:
        - main.weighted_avg(ANY value, ANY weight) -> DOUBLE
        """
        return call_duckdb_function(
            self._WEIGHTED_AVG_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('xor')
    @duckdb_function('year')
    @duckdb_function('yearweek')
    @duckdb_function(symbols=('!__postfix',))
    @duckdb_function(symbols=('%',))
    @duckdb_function(symbols=('&',))
    @duckdb_function(symbols=('*',))
    @duckdb_function(symbols=('**',))
    @duckdb_function(symbols=('+',))
    @duckdb_function(symbols=('-',))
    @duckdb_function(symbols=('/',))
    @duckdb_function(symbols=('//',))
    @duckdb_function(symbols=('<->',))
    @duckdb_function(symbols=('<<',))
    @duckdb_function(symbols=('<=>',))
    @duckdb_function(symbols=('>>',))
    @duckdb_function(symbols=('@',))
    @duckdb_function(symbols=('^',))
    @duckdb_function(symbols=('|',))
    @duckdb_function(symbols=('~',))

        'date_add': 'date_add',
        'fdiv': 'fdiv',
        'fmod': 'fmod',
        'geomean': 'geomean',
        'geometric_mean': 'geometric_mean',
        'get_block_size': 'get_block_size',
        'map_to_pg_oid': 'map_to_pg_oid',
        'md5_number_lower': 'md5_number_lower',
        'md5_number_upper': 'md5_number_upper',
        'pg_my_temp_schema': 'pg_my_temp_schema',
        'round_even': 'round_even',
        'roundbankers': 'roundbankers',
        'wavg': 'wavg',
        'weighted_avg': 'weighted_avg',

    @duckdb_function('__internal_decompress_string')
    @duckdb_function('alias')
    @duckdb_function('array_extract')
    @duckdb_function('array_to_json')
    _ARRAY_TO_STRING_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='array_to_string',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(None, None),
                        parameters=('arr', 'sep'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(CAST(arr AS VARCHAR[]), 'string_agg', sep)",
                    ),
    )
    @duckdb_function('array_to_string')
    def array_to_string(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``array_to_string``.

        Overloads:
        - main.array_to_string(ANY arr, ANY sep) -> VARCHAR
        """
        return call_duckdb_function(
            self._ARRAY_TO_STRING_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _ARRAY_TO_STRING_COMMA_DEFAULT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='array_to_string_comma_default',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(None, None),
                        parameters=('arr', 'sep'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="list_aggr(CAST(arr AS VARCHAR[]), 'string_agg', sep)",
                    ),
    )
    @duckdb_function('array_to_string_comma_default')
    def array_to_string_comma_default(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``array_to_string_comma_default``.

        Overloads:
        - main.array_to_string_comma_default(ANY arr, ANY sep) -> VARCHAR
        """
        return call_duckdb_function(
            self._ARRAY_TO_STRING_COMMA_DEFAULT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('bar')
    @duckdb_function('base64')
    @duckdb_function('bin')
    @duckdb_function('chr')
    @duckdb_function('concat_ws')
    _CURRENT_CATALOG_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='current_catalog',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='main.current_database()',
                    ),
    )
    @duckdb_function('current_catalog')
    def current_catalog(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``current_catalog``.

        Overloads:
        - main.current_catalog() -> VARCHAR
        """
        return call_duckdb_function(
            self._CURRENT_CATALOG_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='current_database',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='"system".main.current_database()',
                    ),
    @duckdb_function('current_database')
        - pg_catalog.current_database() -> VARCHAR
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='current_query',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='"system".main.current_query()',
                    ),
    @duckdb_function('current_query')
        - pg_catalog.current_query() -> VARCHAR
    _CURRENT_ROLE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='current_role',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="'duckdb'",
                    ),
    )
    @duckdb_function('current_role')
    def current_role(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``current_role``.

        Overloads:
        - main.current_role() -> VARCHAR
        """
        return call_duckdb_function(
            self._CURRENT_ROLE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='current_schema',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='"system".main.current_schema()',
                    ),
    @duckdb_function('current_schema')
        - pg_catalog.current_schema() -> VARCHAR
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='current_schemas',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR[]'),
                        parameter_types=(None,),
                        parameters=('include_implicit',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='"system".main.current_schemas(include_implicit)',
                    ),
    @duckdb_function('current_schemas')
        - pg_catalog.current_schemas(ANY include_implicit) -> VARCHAR[]
    _CURRENT_USER_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='current_user',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="'duckdb'",
                    ),
    )
    @duckdb_function('current_user')
    def current_user(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``current_user``.

        Overloads:
        - main.current_user() -> VARCHAR
        """
        return call_duckdb_function(
            self._CURRENT_USER_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('dayname')
    @duckdb_function('decode')
    @duckdb_function('enum_first')
    @duckdb_function('enum_last')
    @duckdb_function('enum_range')
    @duckdb_function('enum_range_boundary')
    @duckdb_function('format')
    @duckdb_function('formatReadableDecimalSize')
    @duckdb_function('formatReadableSize')
    @duckdb_function('format_bytes')
    _FORMAT_PG_TYPE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='format_pg_type',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(None, None),
                        parameters=('logical_type', 'type_name'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="CASE  WHEN ((upper(logical_type) = 'FLOAT')) THEN ('float4') WHEN ((upper(logical_type) = 'DOUBLE')) THEN ('float8') WHEN ((upper(logical_type) = 'DECIMAL')) THEN ('numeric') WHEN ((upper(logical_type) = 'ENUM')) THEN (lower(type_name)) WHEN ((upper(logical_type) = 'VARCHAR')) THEN ('varchar') WHEN ((upper(logical_type) = 'BLOB')) THEN ('bytea') WHEN ((upper(logical_type) = 'TIMESTAMP')) THEN ('timestamp') WHEN ((upper(logical_type) = 'TIME')) THEN ('time') WHEN ((upper(logical_type) = 'TIMESTAMP WITH TIME ZONE')) THEN ('timestamptz') WHEN ((upper(logical_type) = 'TIME WITH TIME ZONE')) THEN ('timetz') WHEN ((upper(logical_type) = 'SMALLINT')) THEN ('int2') WHEN ((upper(logical_type) = 'INTEGER')) THEN ('int4') WHEN ((upper(logical_type) = 'BIGINT')) THEN ('int8') WHEN ((upper(logical_type) = 'BOOLEAN')) THEN ('bool') ELSE lower(logical_type) END",
                    ),
    )
    @duckdb_function('format_pg_type')
    def format_pg_type(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``format_pg_type``.

        Overloads:
        - pg_catalog.format_pg_type(ANY logical_type, ANY type_name) -> VARCHAR
        """
        return call_duckdb_function(
            self._FORMAT_PG_TYPE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _FORMAT_TYPE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='format_type',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(None, None),
                        parameters=('type_oid', 'typemod'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="((SELECT format_pg_type(logical_type, type_name) FROM duckdb_types() AS t WHERE (t.type_oid = type_oid)) || CASE  WHEN ((typemod > 0)) THEN (concat('(', (typemod // 1000), ',', (typemod % 1000), ')')) ELSE '' END)",
                    ),
    )
    @duckdb_function('format_type')
    def format_type(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``format_type``.

        Overloads:
        - pg_catalog.format_type(ANY type_oid, ANY typemod) -> VARCHAR
        """
        return call_duckdb_function(
            self._FORMAT_TYPE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('gen_random_uuid')
    @duckdb_function('hex')
    @duckdb_function('icu_collate_af')
    @duckdb_function('icu_collate_am')
    @duckdb_function('icu_collate_ar')
    @duckdb_function('icu_collate_ar_sa')
    @duckdb_function('icu_collate_as')
    @duckdb_function('icu_collate_az')
    @duckdb_function('icu_collate_be')
    @duckdb_function('icu_collate_bg')
    @duckdb_function('icu_collate_bn')
    @duckdb_function('icu_collate_bo')
    @duckdb_function('icu_collate_br')
    @duckdb_function('icu_collate_bs')
    @duckdb_function('icu_collate_ca')
    @duckdb_function('icu_collate_ceb')
    @duckdb_function('icu_collate_chr')
    @duckdb_function('icu_collate_cs')
    @duckdb_function('icu_collate_cy')
    @duckdb_function('icu_collate_da')
    @duckdb_function('icu_collate_de')
    @duckdb_function('icu_collate_de_at')
    @duckdb_function('icu_collate_dsb')
    @duckdb_function('icu_collate_dz')
    @duckdb_function('icu_collate_ee')
    @duckdb_function('icu_collate_el')
    @duckdb_function('icu_collate_en')
    @duckdb_function('icu_collate_en_us')
    @duckdb_function('icu_collate_eo')
    @duckdb_function('icu_collate_es')
    @duckdb_function('icu_collate_et')
    @duckdb_function('icu_collate_fa')
    @duckdb_function('icu_collate_fa_af')
    @duckdb_function('icu_collate_ff')
    @duckdb_function('icu_collate_fi')
    @duckdb_function('icu_collate_fil')
    @duckdb_function('icu_collate_fo')
    @duckdb_function('icu_collate_fr')
    @duckdb_function('icu_collate_fr_ca')
    @duckdb_function('icu_collate_fy')
    @duckdb_function('icu_collate_ga')
    @duckdb_function('icu_collate_gl')
    @duckdb_function('icu_collate_gu')
    @duckdb_function('icu_collate_ha')
    @duckdb_function('icu_collate_haw')
    @duckdb_function('icu_collate_he')
    @duckdb_function('icu_collate_he_il')
    @duckdb_function('icu_collate_hi')
    @duckdb_function('icu_collate_hr')
    @duckdb_function('icu_collate_hsb')
    @duckdb_function('icu_collate_hu')
    @duckdb_function('icu_collate_hy')
    @duckdb_function('icu_collate_id')
    @duckdb_function('icu_collate_id_id')
    @duckdb_function('icu_collate_ig')
    @duckdb_function('icu_collate_is')
    @duckdb_function('icu_collate_it')
    @duckdb_function('icu_collate_ja')
    @duckdb_function('icu_collate_ka')
    @duckdb_function('icu_collate_kk')
    @duckdb_function('icu_collate_kl')
    @duckdb_function('icu_collate_km')
    @duckdb_function('icu_collate_kn')
    @duckdb_function('icu_collate_ko')
    @duckdb_function('icu_collate_kok')
    @duckdb_function('icu_collate_ku')
    @duckdb_function('icu_collate_ky')
    @duckdb_function('icu_collate_lb')
    @duckdb_function('icu_collate_lkt')
    @duckdb_function('icu_collate_ln')
    @duckdb_function('icu_collate_lo')
    @duckdb_function('icu_collate_lt')
    @duckdb_function('icu_collate_lv')
    @duckdb_function('icu_collate_mk')
    @duckdb_function('icu_collate_ml')
    @duckdb_function('icu_collate_mn')
    @duckdb_function('icu_collate_mr')
    @duckdb_function('icu_collate_ms')
    @duckdb_function('icu_collate_mt')
    @duckdb_function('icu_collate_my')
    @duckdb_function('icu_collate_nb')
    @duckdb_function('icu_collate_nb_no')
    @duckdb_function('icu_collate_ne')
    @duckdb_function('icu_collate_nl')
    @duckdb_function('icu_collate_nn')
    @duckdb_function('icu_collate_noaccent')
    @duckdb_function('icu_collate_om')
    @duckdb_function('icu_collate_or')
    @duckdb_function('icu_collate_pa')
    @duckdb_function('icu_collate_pa_in')
    @duckdb_function('icu_collate_pl')
    @duckdb_function('icu_collate_ps')
    @duckdb_function('icu_collate_pt')
    @duckdb_function('icu_collate_ro')
    @duckdb_function('icu_collate_ru')
    @duckdb_function('icu_collate_sa')
    @duckdb_function('icu_collate_se')
    @duckdb_function('icu_collate_si')
    @duckdb_function('icu_collate_sk')
    @duckdb_function('icu_collate_sl')
    @duckdb_function('icu_collate_smn')
    @duckdb_function('icu_collate_sq')
    @duckdb_function('icu_collate_sr')
    @duckdb_function('icu_collate_sr_ba')
    @duckdb_function('icu_collate_sr_me')
    @duckdb_function('icu_collate_sr_rs')
    @duckdb_function('icu_collate_sv')
    @duckdb_function('icu_collate_sw')
    @duckdb_function('icu_collate_ta')
    @duckdb_function('icu_collate_te')
    @duckdb_function('icu_collate_th')
    @duckdb_function('icu_collate_tk')
    @duckdb_function('icu_collate_to')
    @duckdb_function('icu_collate_tr')
    @duckdb_function('icu_collate_ug')
    @duckdb_function('icu_collate_uk')
    @duckdb_function('icu_collate_ur')
    @duckdb_function('icu_collate_uz')
    @duckdb_function('icu_collate_vi')
    @duckdb_function('icu_collate_wae')
    @duckdb_function('icu_collate_wo')
    @duckdb_function('icu_collate_xh')
    @duckdb_function('icu_collate_yi')
    @duckdb_function('icu_collate_yo')
    @duckdb_function('icu_collate_yue')
    @duckdb_function('icu_collate_yue_cn')
    @duckdb_function('icu_collate_zh')
    @duckdb_function('icu_collate_zh_cn')
    @duckdb_function('icu_collate_zh_hk')
    @duckdb_function('icu_collate_zh_mo')
    @duckdb_function('icu_collate_zh_sg')
    @duckdb_function('icu_collate_zh_tw')
    @duckdb_function('icu_collate_zu')
    @duckdb_function('icu_sort_key')
    _JSON_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='json',
                        function_type=function_type,
                        return_type=parse_type('JSON'),
                        parameter_types=(None,),
                        parameters=('x',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="json_extract(x, '$')",
                    ),
    )
    @duckdb_function('json')
    def json(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``json``.

        Overloads:
        - main.json(ANY x) -> JSON
        """
        return call_duckdb_function(
            self._JSON_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('json_array')
    @duckdb_function('json_deserialize_sql')
    @duckdb_function('json_extract')
    @duckdb_function('json_extract_path')
    @duckdb_function('json_extract_path_text')
    @duckdb_function('json_extract_string')
    _JSON_GROUP_ARRAY_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='json_group_array',
                        function_type=function_type,
                        return_type=parse_type('JSON'),
                        parameter_types=(None,),
                        parameters=('x',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='CAST(((\'[\' || string_agg(CASE  WHEN ((x IS NULL)) THEN (CAST(\'null\' AS "JSON")) ELSE to_json(x) END, \',\')) || \']\') AS "JSON")',
                    ),
    )
    @duckdb_function('json_group_array')
    def json_group_array(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``json_group_array``.

        Overloads:
        - main.json_group_array(ANY x) -> JSON
        """
        return call_duckdb_function(
            self._JSON_GROUP_ARRAY_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _JSON_GROUP_OBJECT_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='json_group_object',
                        function_type=function_type,
                        return_type=parse_type('JSON'),
                        parameter_types=(None, None),
                        parameters=('n', 'v'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='CAST(((\'{\' || string_agg(((to_json(CAST(n AS VARCHAR)) || \':\') || CASE  WHEN ((v IS NULL)) THEN (CAST(\'null\' AS "JSON")) ELSE to_json(v) END), \',\')) || \'}\') AS "JSON")',
                    ),
    )
    @duckdb_function('json_group_object')
    def json_group_object(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``json_group_object``.

        Overloads:
        - main.json_group_object(ANY n, ANY v) -> JSON
        """
        return call_duckdb_function(
            self._JSON_GROUP_OBJECT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _JSON_GROUP_STRUCTURE_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='json_group_structure',
                        function_type=function_type,
                        return_type=parse_type('JSON'),
                        parameter_types=(None,),
                        parameters=('x',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='(json_structure(json_group_array(x)) -> 0)',
                    ),
    )
    @duckdb_function('json_group_structure')
    def json_group_structure(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``json_group_structure``.

        Overloads:
        - main.json_group_structure(ANY x) -> JSON
        """
        return call_duckdb_function(
            self._JSON_GROUP_STRUCTURE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('json_keys')
    @duckdb_function('json_merge_patch')
    @duckdb_function('json_object')
    @duckdb_function('json_pretty')
    @duckdb_function('json_quote')
    @duckdb_function('json_serialize_plan')
    @duckdb_function('json_serialize_sql')
    @duckdb_function('json_structure')
    @duckdb_function('json_type')
    @duckdb_function('json_value')
    @duckdb_function('lcase')
    @duckdb_function('left')
    @duckdb_function('left_grapheme')
    @duckdb_function('list_element')
    @duckdb_function('list_extract')
    @duckdb_function('lower')
    @duckdb_function('lpad')
    @duckdb_function('ltrim')
    @duckdb_function('md5')
    @duckdb_function('monthname')
    @duckdb_function('nfc_normalize')
    @duckdb_function('parse_dirname')
    @duckdb_function('parse_dirpath')
    @duckdb_function('parse_filename')
    @duckdb_function('parse_path')
    _PG_GET_CONSTRAINTDEF_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_get_constraintdef',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(None,),
                        parameters=('constraint_oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='(SELECT constraint_text FROM duckdb_constraints() AS d_constraint WHERE ((d_constraint.table_oid = (constraint_oid // 1000000)) AND (d_constraint.constraint_index = (constraint_oid % 1000000))))',
                    ),
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_get_constraintdef',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(None, None),
                        parameters=('constraint_oid', 'pretty_bool'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='pg_get_constraintdef(constraint_oid)',
                    ),
    )
    @duckdb_function('pg_get_constraintdef')
    def pg_get_constraintdef(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``pg_get_constraintdef``.

        Overloads:
        - pg_catalog.pg_get_constraintdef(ANY constraint_oid) -> VARCHAR
        - pg_catalog.pg_get_constraintdef(ANY constraint_oid, ANY pretty_bool) -> VARCHAR
        """
        return call_duckdb_function(
            self._PG_GET_CONSTRAINTDEF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_GET_VIEWDEF_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_get_viewdef',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(None,),
                        parameters=('oid',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='(SELECT "sql" FROM duckdb_views() AS v WHERE (v.view_oid = oid))',
                    ),
    )
    @duckdb_function('pg_get_viewdef')
    def pg_get_viewdef(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``pg_get_viewdef``.

        Overloads:
        - pg_catalog.pg_get_viewdef(ANY oid) -> VARCHAR
        """
        return call_duckdb_function(
            self._PG_GET_VIEWDEF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_SIZE_PRETTY_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_size_pretty',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(None,),
                        parameters=('bytes',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='format_bytes(bytes)',
                    ),
    )
    @duckdb_function('pg_size_pretty')
    def pg_size_pretty(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``pg_size_pretty``.

        Overloads:
        - pg_catalog.pg_size_pretty(ANY bytes) -> VARCHAR
        """
        return call_duckdb_function(
            self._PG_SIZE_PRETTY_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    _PG_TYPEOF_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='pg_catalog',
                        function_name='pg_typeof',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(None,),
                        parameters=('expression',),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='lower(typeof(expression))',
                    ),
    )
    @duckdb_function('pg_typeof')
    def pg_typeof(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``pg_typeof``.

        Overloads:
        - pg_catalog.pg_typeof(ANY expression) -> VARCHAR
        """
        return call_duckdb_function(
            self._PG_TYPEOF_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('printf')
    @duckdb_function('regexp_escape')
    @duckdb_function('regexp_extract')
    @duckdb_function('regexp_extract_all')
    @duckdb_function('regexp_replace')
    @duckdb_function('regexp_split_to_array')
    @duckdb_function('repeat')
    @duckdb_function('replace')
    @duckdb_function('reverse')
    @duckdb_function('right')
    @duckdb_function('right_grapheme')
    @duckdb_function('row_to_json')
    @duckdb_function('rpad')
    @duckdb_function('rtrim')
    _SESSION_USER_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='session_user',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition="'duckdb'",
                    ),
    )
    @duckdb_function('session_user')
    def session_user(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``session_user``.

        Overloads:
        - main.session_user() -> VARCHAR
        """
        return call_duckdb_function(
            self._SESSION_USER_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('sha1')
    @duckdb_function('sha256')
    @duckdb_function('split')
    _SPLIT_PART_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='split_part',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(None, None, None),
                        parameters=('string', 'delimiter', 'position'),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='CASE  WHEN (((string IS NOT NULL) AND ("delimiter" IS NOT NULL) AND ("position" IS NOT NULL))) THEN (COALESCE(string_split(string, "delimiter")["position"], \'\')) ELSE NULL END',
                    ),
    )
    @duckdb_function('split_part')
    def split_part(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``split_part``.

        Overloads:
        - main.split_part(ANY string, ANY delimiter, ANY position) -> VARCHAR
        """
        return call_duckdb_function(
            self._SPLIT_PART_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('stats')
    @duckdb_function('str_split')
    @duckdb_function('str_split_regex')
    @duckdb_function('strftime')
    @duckdb_function('string_split')
    @duckdb_function('string_split_regex')
    @duckdb_function('string_to_array')
    @duckdb_function('strip_accents')
    @duckdb_function('substr')
    @duckdb_function('substring')
    @duckdb_function('substring_grapheme')
    @duckdb_function('to_base')
    @duckdb_function('to_base64')
    @duckdb_function('to_binary')
    @duckdb_function('to_hex')
    @duckdb_function('to_json')
    @duckdb_function('translate')
    @duckdb_function('trim')
    @duckdb_function('typeof')
    @duckdb_function('ucase')
    @duckdb_function('upper')
    @duckdb_function('url_decode')
    @duckdb_function('url_encode')
    _USER_SIGNATURES: ClassVar[tuple[DuckDBFunctionDefinition, ...]] = (
                    DuckDBFunctionDefinition(
                        schema_name='main',
                        function_name='user',
                        function_type=function_type,
                        return_type=parse_type('VARCHAR'),
                        parameter_types=(),
                        parameters=(),
                        varargs=None,
                        description=None,
                        comment=None,
                        macro_definition='current_user',
                    ),
    )
    @duckdb_function('user')
    def user(self, *operands: object) -> VarcharExpression:
        """Call DuckDB function ``user``.

        Overloads:
        - main.user() -> VARCHAR
        """
        return call_duckdb_function(
            self._USER_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
        )
    @duckdb_function('uuid')
    @duckdb_function('uuidv4')
    @duckdb_function('uuidv7')
    @duckdb_function('variant_typeof')
    @duckdb_function('vector_type')
    @duckdb_function('version')
    @duckdb_function(symbols=('->>',))

        'array_to_string': 'array_to_string',
        'array_to_string_comma_default': 'array_to_string_comma_default',
        'current_catalog': 'current_catalog',
        'current_role': 'current_role',
        'current_user': 'current_user',
        'format_pg_type': 'format_pg_type',
        'format_type': 'format_type',
        'json': 'json',
        'json_group_array': 'json_group_array',
        'json_group_object': 'json_group_object',
        'json_group_structure': 'json_group_structure',
        'pg_get_constraintdef': 'pg_get_constraintdef',
        'pg_get_viewdef': 'pg_get_viewdef',
        'pg_size_pretty': 'pg_size_pretty',
        'pg_typeof': 'pg_typeof',
        'session_user': 'session_user',
        'split_part': 'split_part',
        'user': 'user',

