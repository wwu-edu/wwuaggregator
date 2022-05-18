# Copyright 2022 WWU-OIE, Western Washington University
# SPDX-License-Identifier: MIT License
import polars as pl

class WWU_Aggregator:
	"""
	This class aggregates datasets according to standard and custom functions.

	Example usage:
		import pandas as pd
		from wwu-aggregator import WWU_Aggregator

		p = WWU_Aggregator()

		data = [{'name': 'John', 'year': '2019', 'subject': 'ECON', 'grade': 90},
				{'name': 'John', 'year': '2020', 'subject': 'ECON', 'grade': 94},
				{'name': 'John', 'year': '2019', 'subject': 'STAT', 'grade': 96},
				{'name': 'John', 'year': '2020', 'subject': 'STAT', 'grade': 98},
				{'name': 'Andy', 'year': '2019', 'subject': 'MUSIC', 'grade': 70},
				{'name': 'Andy', 'year': '2020', 'subject': 'MUSIC', 'grade': 72},
				{'name': 'Andy', 'year': '2019', 'subject': 'PHIL', 'grade': 74},
				{'name': 'Andy', 'year': '2020', 'subject': 'PHIL', 'grade': 76},
				{'name': 'Beth', 'year': '2019', 'subject': 'PHIL', 'grade': 85},
				{'name': 'Beth', 'year': '2020', 'subject': 'PHIL', 'grade': 83},
				{'name': 'Beth', 'year': '2019', 'subject': 'COMPSCI', 'grade': 87},
				{'name': 'Beth', 'year': '2020', 'subject': 'COMPSCI', 'grade': 89}]
		
		df = pd.DataFrame(data)

		r = p.operations(
			[
				{"operation":"mean","column":"grade"},
			]
		) \
		.dimensions_constant([['name']]) \
		.dimensions_change([['subject'], ['year'], ['subject', 'year']]) \
		.dataframe(df) \
		.execute()

		print(r)

	"""
	def __init__(self):
		self.operations_var = None					# initialize
		self.groupby_var = None						# initialize
		self.dimensions_constant_var = None			# initialize
		self.dimensions_change_var = None			# initialize
		self.dataframe_var = None					# initialize

	############################################################################################
	# PRIVATE METHODS
	############################################################################################
	def _convert_dimension_to_string(self, dimension):
		"""
		Convert None/NaN-values in dimension columns to placeholder string (otherwise, the Pandas groupby-function will filter them out when aggregating).

		Parameters:
			dimension(str):  the name of the dimension in self.dataframe_var that needs to be converted to a string.
		"""
		# If we pulled this dimension from a database column which had type Int but some records were NULL, Python will convert it to float64 for performance reasons (https://pandas.pydata.org/pandas-docs/stable/user_guide/gotchas.html#nan-integer-na-values-and-na-type-promotions).
		# This becomes problematic when we convert these floats to strings because it appends ".0" to the string-value (i.e., survey id 332674 becomes 332674.0 and that looks weird).
		# Therefore, we strip off the ".0" from the string if we detect that the original dataframe was type float64 and it contained NULLs.
		convert_float = False  # initialize
		if self.dataframe_var.dtypes[dimension] == "float64" and self.dataframe_var.isnull().values.any():
			convert_float = True

		self.dataframe_var[dimension] = self.dataframe_var[dimension].fillna('(no value)')

		if convert_float:
			self.dataframe_var[dimension] = self.dataframe_var[dimension].astype(str).str.rstrip(".0")

	############################################################################################
	# CUSTOM AGGREGATION METHODS
	############################################################################################
	def _agg_of_complement(self, df, col, op, groupby_columns, complement_id_columns):
		"""
		Performs aggregations of an individual against peers (ex. a single faculty member against others).

		Parameters:
			df (dataframe): dataframe containing data
			col (string):   name of the column containing values over which the aggregation-function should be applied
			op (string):  aggregation to perform (ex. mean, median, etc.)
			groupby_columns (list):  list of columns that each row of output should be aggregated by
			complement_id_columns (list):  list of columns that identify the individual versus its peers (i.e., criteria for finding the complement)

		Returns:
			pandas.Series: Series of aggregated values (having groupby_columns as the index)
		"""
		### INITIALIZATION
		key_set = set()  # initialize
		aggregation_results_dict = dict() # initialize

		df_aggregated = None  # initialize
		remove_extranneous_rows = False  # initialize

		### DETERMINE THE "KEYS" (i.e., the things that we want to evaluate the complement of)
		# the complement columns should be used to label the row, but should not be used in the actual aggregation-calculation.  Preserve the order.  (important for assigning column names at the end)
		# Deep copy of the list
		groupby_columns_adj = list()
		for groupby_col in groupby_columns:
			groupby_columns_adj.append(groupby_col)

		for groupby_col in groupby_columns:
			if groupby_col in complement_id_columns:
				groupby_columns_adj.remove(groupby_col)

		# generating a list of key-values for the complements
		for rw in df.to_dicts():
			el_lst = list()
			for itm in complement_id_columns:
				el_lst.append(rw[itm])

			key_set.add(tuple(el_lst))

		### ITERATE OVER THE KEYS (processing the aggregation of the key's complement in each iteration)
		for key_tuple in key_set:
			# get the tuple into a dict so it can be used to filter the dataframe
			dct = dict()
			i=0
			for itm in complement_id_columns:
				dct[itm] = key_tuple[i]
				i = i + 1

			# remove rows from the dataframe in order to achieve the complement
			df_complement = df.clone()

			if "df_complement_index" in df_complement.columns:
				raise Exception("df_complement already contains a column named df_complement_index.  Aborting.")
			if "df_complement_keep" in df_complement.columns:
				raise Exception("df_complement already contains a column named df_complement_keep.  Aborting.")
			
			df_complement = df_complement.with_column(pl.concat_list(complement_id_columns).alias("df_complement_index"))  # adding a column which contains a list of the elements in the complement_id

			list_key_tuple = list(key_tuple)  # convert our current tuple to a list so that we can compare a list to a list

			# Now that we have the "index" of our complement_id values as a column in our dataframe, we need to compare this to the tuple of our current iteration.
			# By doing so, we can take out the rows that match our tuple and the remaining rows will be the complement of the tuple.
			# However, we do not appear to be able to compare a collection to a collection using the filter-method of Polars; it seemed to want a single value.
			# Therefore, we are iterating the dataframe on a row-by-row basis to populate a True/False series that will be used as a mask for filtering rows later.
			# If the row matches our tuple, its keep_mask-value is False; otherwise, it is true.
			# Then, we append the True/False series to our dataframe.  And finally, we filter the dataframe according to this True/False column.
			# I expect we can find a more-performant way of doing this, and encourage us to do so if time allows and we experience a need to make things faster.
			keep_mask = list()  # initialize
			for rw in df_complement.rows():
				if rw[df_complement.find_idx_by_name("df_complement_index")] == list_key_tuple:
					keep_mask.append(False)
				else:
					keep_mask.append(True)

			# Add the True/False series to the dataframe
			df_complement = df_complement.with_column(pl.Series("df_complement_keep", keep_mask))

			# filter the dataframe to just the complement
			df_complement = df_complement.filter(pl.col("df_complement_keep") == True)

			# perform the aggregation over the complement
			if groupby_columns == complement_id_columns:
				### SIMPLE CASE:  there are no grouping-dimensions other than the complement_id columns
				# We are hand-crafting a dict of lists (essentially a dict of Series) that will be cast as a Polars DataFrame after aggregation.
				if len(aggregation_results_dict) < 1:
					if "aggregation_results" in complement_id_columns:
						raise Exception("Found entry 'aggregation_results' in complement_id_columns. Aborting.")

					# initialize the dict of lists
					aggregation_results_dict["aggregation_results"] = list()
					for complement_id_column in complement_id_columns:
						aggregation_results_dict[complement_id_column] = list()

				# populate this round's key_tuple values in the dict.  We will add the complement's aggregation later on.			
				i = 0
				while i < len(complement_id_columns):
					aggregation_results_dict[complement_id_columns[i]].append(key_tuple[i])
					i = i + 1

				# perform the aggregation
				if op == "count":
					aggregation_results_dict["aggregation_results"].append(df_complement[col].count())
				elif op == "count_distinct":
					aggregation_results_dict["aggregation_results"].append(df_complement[col].n_unique())
				elif op == "max":
					aggregation_results_dict["aggregation_results"].append(df_complement[col].max())
				elif op == "mean":
					aggregation_results_dict["aggregation_results"].append(df_complement[col].mean())
				elif op == "median":
					aggregation_results_dict["aggregation_results"].append(df_complement[col].median())
				elif op == "min":
					aggregation_results_dict["aggregation_results"].append(df_complement[col].min())
				elif op == "std":
					aggregation_results_dict["aggregation_results"].append(df_complement[col].std())
				elif op == "sum":
					aggregation_results_dict["aggregation_results"].append(df_complement[col].sum())
				else:
					raise ValueError(f"Unknown op: {op}")

				# convert dict of lists to Polars DataFrame
				df_aggregated = pl.DataFrame(aggregation_results_dict)

			else:
				### SOPHISTICATED CASE:  there are grouping-dimensions in addition to the complement_id columns
				remove_extranneous_rows = True

				# perform the aggregation
				if op == "count":
					ans = (df_complement.groupby(groupby_columns_adj).agg([pl.col(col).count()]))
				elif op == "count_distinct":
					ans = (df_complement.groupby(groupby_columns_adj).agg([pl.col(col).n_unique()]))
				elif op == "max":
					ans = (df_complement.groupby(groupby_columns_adj).agg([pl.col(col).max()]))
				elif op == "mean":
					ans = (df_complement.groupby(groupby_columns_adj).agg([pl.col(col).mean()]))
				elif op == "median":
					ans = (df_complement.groupby(groupby_columns_adj).agg([pl.col(col).median()]))
				elif op == "min":
					ans = (df_complement.groupby(groupby_columns_adj).agg([pl.col(col).min()]))
				elif op == "std":
					ans = (df_complement.groupby(groupby_columns_adj).agg([pl.col(col).std()]))
				elif op == "sum":
					ans = (df_complement.groupby(groupby_columns_adj).agg([pl.col(col).sum()]))
				else:
					raise ValueError(f"Unknown op: {op}")

				ans = ans.rename({col:"aggregation_results"})  # names the aggregation-column

				# ans will be a dataframe comprised of the non-complement grouping-dimensions and the aggregated values.  
				# We need to add the tuple-values to the dataframe since it is serving as the "index" of the row.
				#  (i.e., it tells us what this is the complement of)
				# Ultimately, every row will contain the value of the aggregation, the "index"-columns used to determine the complement, and
				# the columns used for additional groupings within the "index".
				complement_id_column_idx = 0
				for complement_id_column in complement_id_columns:
					ans = ans.with_column(pl.lit(key_tuple[complement_id_column_idx]).alias(complement_id_column))
					complement_id_column_idx = complement_id_column_idx + 1

				# tack this key_tuple iteration onto the dataframe
				if df_aggregated is None:
					df_aggregated = ans
				else:
					df_aggregated = pl.concat([df_aggregated, ans], True, 'vertical')

		### CLEAN UP
		# The process for the sophisticated case has the capability of generating rows for complements of which there was no original subject.
		# As an example, consider the case where College A has only 2021 data, but Colleges B & C have 2021 and 2022 data.  The resultset here will show
		# College A as having had 2022 data, even though this is not the case.  This occurs because the values of the years-dimension is determined by the complement
		# rather than the original subject (i.e., the current key_tuple).
		# Filter-out these rows by validating that the current key was present in the original set.
		if remove_extranneous_rows:
			df_original_keys = df.select(groupby_columns).distinct()
			df_aggregated = df_aggregated.join(df_original_keys, left_on=groupby_columns, right_on=groupby_columns)

		### RETURN THE RESULTS
		# In cases where the complement_id_columns are a generalization of the groupby_columns (such as "college" for "college,department"), 
		# you get a DataFrame with zero rows returned when it filters out non-present rows.
		# Thus, we return None here in that instance instead of the empty DataFrame.
		r = None
		if len(df_aggregated) > 0:
			r = df_aggregated

		return r


	############################################################################################
	# CLASS OPERATIONS METHODS
	############################################################################################
	def operations(self, list):
		"""
		Sets the columns that should be returned by the execute-method.

		Parameters:
			list (list): list of dictionaries for aggregated columns

		Returns:
			Self: allows for method-chaining
		"""
		self.operations_var = list
		return self

	def dimensions_constant(self, dimensions):
		"""
		Declares the lists of dimensions to be used in every aggregation in the execute-method.

		Parameters:
			dimensions (list): list of lists (for columns)

		Returns:
			Self: allows for method-chaining
		"""
		# validate that the parameter is in fact a list of lists
		if type(dimensions) != list:
			raise Exception(f"Parameter for dimensions_constant must be a list of lists (received {dimensions})")

		for dim in dimensions:
			if type(dim) != list:
				raise Exception(f"Parameter for dimensions_constant must be a list of lists (received element that is not a list: {dim})")

		self.dimensions_constant_var = dimensions
		return self

	def dimensions_change(self, dimensions):
		"""
		Declares the lists of dimensions to be cross joined with dimensions_constant to generate the aggregation sets in the execute-method.

		Parameters:
			dimensions (list): list of lists (for columns)

		Returns:
			Self: allows for method-chaining
		"""
		if type(dimensions) != list:
			raise Exception(f"Parameter for dimensions_change must be a list of lists (received {dimensions})")

		for dim in dimensions:
			if type(dim) != list:
				raise Exception(f"Parameter for dimensions_change must be a list of lists (received element that is not a list: {dim})")

		self.dimensions_change_var = dimensions
		return self

	def dataframe(self, dataframe):
		"""
		Declares the dataframe used as the data source for aggregation.

		Parameters:
			dataframe (dataframe): data source for aggregation

		Returns:
			Self: allows for method-chaining
		"""
		self.dataframe_var = dataframe
		return self

	def execute(self):
		"""
		Performs the aggregation-process once the operations, dataframe, and groupby have been configured.

		Parameters: none

		Returns:
			dataframe: the processed dataframe
		"""
		# Validation
		# Validate the configuration of the class
		if self.operations_var is None or self.dataframe_var is None or self.dimensions_constant_var is None:
			raise Exception("Either operations, dataframe, or dimensions_constant has not been set.")

		# Validate that constant-dimensions and change-dimensions do not have overlapping elements; these will screw up how it evaluates "aggregation_dimensions_names_set" because it will match a term in the set according to the constant-dimension rather than according to the change-dimension.
		constant_set = set()
		for lst in self.dimensions_constant_var:
			for sub_lst in lst:
				constant_set.add(sub_lst)

		# Convert None/NaN-values in constant-dimension columns to placeholder string (so we can get friendly "(no value)"-entries and avoid INTs converted to str getting ".0" at the end)
		for constant_itm in constant_set:
			self._convert_dimension_to_string(dimension=constant_itm)

		if self.dimensions_change_var is not None:
			change_set = set()
			for lst in self.dimensions_change_var:
				for sub_lst in lst:
					change_set.add(sub_lst)
					
			if len(constant_set.intersection(change_set)) > 0:
				raise ValueError("Constant dimension elements and change dimension elements must be mutually exclusive.")
	
		# Generate aggregation dimensions based off constant and change dimension lists.
		# If no change dimensions are passed, then just use the constant dimensions.
		if self.dimensions_change_var is None:
			self.groupby_var = self.dimensions_constant_var
		# Else generate cartesian product of constant dimensions lists and change dimensions lists.
		else:
			dim_list = list()  # initialize
			# iterate the list
			for change_list in self.dimensions_change_var:
				# Since we are already iterating change-dimensions here, let's also convert None/NaN to placeholder string (so we can get friendly "(no value)"-entries and avoid INTs converted to str getting ".0" at the end)
				for change_itm in change_list:
					self._convert_dimension_to_string(dimension=change_itm)

				# append the list to each entry in the list of lists, and add it to the final list
				for lst in self.dimensions_constant_var:
					temp_list = list()
					for itm in lst:
						temp_list.append(itm)
					for change_itm in change_list:
						temp_list.append(change_itm)
					dim_list.append(temp_list)
			# add in the original entries from dim_list_constant
			for lst in self.dimensions_constant_var:
				dim_list.append(lst)
			self.groupby_var = dim_list

		##########################################################
		# AGGREGATION
		##########################################################
		return_dataset = None  # initialize

		groupby_var = self.groupby_var
		dimensions_change_var = self.dimensions_change_var
		operations_var = self.operations_var
		dataframe_var = self.dataframe_var

		# convert DataFrame to polars for faster processing
		dataframe_var = pl.DataFrame(dataframe_var)

		# iterate through all of the grouping sets, and only pull the ones that should be processed by this thread
		for i in range(len(groupby_var)):
			dimension_dataset = None #initialize

			# loop through aggregation operations
			for op in operations_var:
				operation = op["operation"]
				column = op["column"]
				alias = f'{op["column"]}_{op["operation"]}'

				if str(operation).endswith("_of_complement"):
					if "of_complement" not in op:
						raise ValueError(f'The operation {operation} requires an "of_complement" parameter in order to determine what constitutes the complement, but none was provided.')
					of_complement = op["of_complement"]

					if type(of_complement) is not list:
						raise ValueError(f"of_complement value for {operation} operation must be a list.  Received {type(of_complement)}: {of_complement}")

					# For _of_complement operations, validate that the complement_id_columns are in the groupby_columns.
					# If not, then the column should be blank values in the final dataset.
					missing_complement_id_fields = set(of_complement) - set(groupby_var[i])
					if len(missing_complement_id_fields) > 0:
						continue

					# perform the aggregation
					groupby_dataset = self._agg_of_complement(df = dataframe_var, col = column, op = str(operation).split("_")[0], groupby_columns = groupby_var[i], complement_id_columns = of_complement)

					# if there are no results (such as when the groupby_var[i] is a specialization of "of_complement"), just continue the loop
					if groupby_dataset is None:
						continue
					else:
						column = "aggregation_results"  # re-assigned for future-processing of column-renaming
				elif operation == "count":
					groupby_dataset = (dataframe_var.groupby(groupby_var[i]).agg([pl.col(column).count()]))
				elif operation == "count_distinct":
					groupby_dataset = (dataframe_var.groupby(groupby_var[i]).agg([pl.col(column).n_unique()]))
				elif operation == "max":
					groupby_dataset = (dataframe_var.groupby(groupby_var[i]).agg([pl.col(column).max()]))
				elif operation == "mean":
					groupby_dataset = (dataframe_var.groupby(groupby_var[i]).agg([pl.col(column).mean()]))
				elif operation == "median":
					groupby_dataset = (dataframe_var.groupby(groupby_var[i]).agg([pl.col(column).median()]))
				elif operation == "min":
					groupby_dataset = (dataframe_var.groupby(groupby_var[i]).agg([pl.col(column).min()]))
				elif operation == "percent_of_total_categorical":
					# For percent_of_total_categorical, validate that the measure column is in the groupby_columns.
					# If not, then the column should be blank values in the final dataset.
					if column not in groupby_var[i]:
						continue

					# create a dataframe of counts per value of the column
					if "totals_dataset_count" in dataframe_var.columns:
						raise ValueError('Dataframe already contains column named "totals_dataset_count"')
					totals_counts = dataframe_var.groupby(groupby_var[i]).agg(pl.count()).rename({"count":"totals_dataset_count"})

					# create dataframe of parent-group counts
					groupby_without_col = list()
					for el in groupby_var[i]:
						if el != column:
							groupby_without_col.append(el)

					if len(groupby_without_col) > 0:
						groupby_without_col_counts = dataframe_var.groupby(groupby_without_col).agg(pl.count())

						if "groupby_without_col_counts_dataset_size" in dataframe_var.columns:
							raise ValueError('Dataframe already contains column named "groupby_without_col_counts_dataset_size"')

						groupby_without_col_counts_dataset = groupby_without_col_counts.rename({"count":"groupby_without_col_counts_dataset_size"})  # names the aggregation-column

						# merge the two dataframes
						groupby_df = totals_counts.join(groupby_without_col_counts_dataset, left_on=groupby_without_col, right_on=groupby_without_col)


						# calculate the percent of total
						if "percent_of_total_categorical" in groupby_df.columns:
							raise ValueError('Dataframe already contains column named "percent_of_total_categorical"')
						
						groupby_dataset = groupby_df.with_column(pl.lit(groupby_df["totals_dataset_count"] / groupby_df["groupby_without_col_counts_dataset_size"]).alias("percent_of_total_categorical"))
						groupby_dataset = groupby_dataset.drop("groupby_without_col_counts_dataset_size")  # drop column added to the dataframe during processing

					else:
						# if the only column in the grouping is the one we are trying to compute percent of total for, then calculate the size for the whole dataframe
						len_dataframe_var = len(dataframe_var)

						if "percent_of_total_categorical" in totals_counts.columns:
							raise ValueError('Dataframe already contains column named "percent_of_total_categorical"')

						groupby_dataset = totals_counts.with_column(pl.lit(totals_counts["totals_dataset_count"] / len_dataframe_var).alias("percent_of_total_categorical"))

					# clean-up steps for later processing
					column = "percent_of_total_categorical"  # ensures that the correct column is renamed with the alias further down in the code
					groupby_dataset = groupby_dataset.drop("totals_dataset_count")  # drop column added to the dataframe during processing

				elif operation == "percent_of_total_numeric":
					if "of_total" not in op:
						raise ValueError(f'The operation {operation} requires an "of_total" parameter in order to determine the total we are calculating respective of, but none was provided.  If you wish to calculate the total with respect to the entire dataset, you may use the wildcard "*".')
					of_total = op["of_total"]

					if of_total == "*" or of_total == ["*"]:
						# This use-case is when you want the percent of total over all records.
						# calculate the sum of the column over the whole dataframe
						dataframe_var_sum = dataframe_var[column].sum()

						# get dataframe summed at level of groupby_var
						totals_sums = dataframe_var.groupby(groupby_var[i]).agg(pl.sum(column))

						# calculate percent of total
						if "percent_of_total_numeric" in totals_sums.columns:
							raise ValueError('Dataframe already contains column named "percent_of_total_numeric"')

						groupby_dataset = totals_sums.with_column(pl.lit(totals_sums[column] / dataframe_var_sum).alias("percent_of_total_numeric"))

						# clean-up steps for later processing
						groupby_dataset = groupby_dataset.drop("percent_of_total_numeric")  # drop column added to the dataframe during processing

					else:
						# This use-case is when you want the percent of total for a specific grouping (such as percent of total for the year; each year sums up to 100%).
						if type(of_total) is not list:
							raise ValueError(f"of_total value for percent_of_total_numeric operation must be a list.  Received {type(of_total)}: {of_total}")

						if column in of_total:
							# we are computing summation for the column, so we cannot also list it separately as a value of the grouping
							raise ValueError(f"Column for calculation ({column}) cannot be contained in list of_total: {of_total}")

						# check to make sure that the of_total columns are actually in the dataframe
						for el in of_total:
							if el not in dataframe_var.columns:
								raise ValueError(f"Element '{el}' in of_total {of_total} is not found in dataframe columns {dataframe_var.columns}")

						# the of_total group must be a subset of the groupby_var; otherwise, you would not be able to represent its values on the row.
						check_lst = set(of_total) - set(groupby_var[i])
						if len(check_lst) > 0:
							continue

						# compute the sum for the of_total groups
						of_total_sums = dataframe_var.groupby(of_total).agg(pl.sum(column)).rename({column:"of_total_sums_sum"})

						# compute the sum for the groupby_var groups
						groupby_var_sums = dataframe_var.groupby(groupby_var[i]).agg(pl.sum(column)).rename({column:"groupby_var_sums_sum"})

						# merge the two dfs
						merged_df = of_total_sums.join(groupby_var_sums, left_on=of_total, right_on=of_total)

						# adjust the alias for the column (in case there are multiple cases where operation percent_of_total_numeric is performed using different of_total values)
						for el in of_total:
							alias = alias + "_" + el
						
						if alias in merged_df.columns:
							raise ValueError(f'Dataframe already contains column named "{alias}"')

						# calculate the percentage
						groupby_dataset = merged_df.with_column(pl.lit(merged_df["groupby_var_sums_sum"] / merged_df["of_total_sums_sum"]).alias(alias))

						# clean-up steps for later processing
						column = alias  # ensures an error does not occur further down in the code
						groupby_dataset = groupby_dataset.drop("groupby_var_sums_sum")  # drop column added to the dataframe during processing
						groupby_dataset = groupby_dataset.drop("of_total_sums_sum")  # drop column added to the dataframe during processing


				elif operation == "std":
					groupby_dataset = (dataframe_var.groupby(groupby_var[i]).agg([pl.col(column).std()]))
				elif operation == "sum":
					groupby_dataset = (dataframe_var.groupby(groupby_var[i]).agg([pl.col(column).sum()]))
				else:
					raise NotImplementedError(f"Operation {operation} is not defined.")

				# name the aggregation-column
				groupby_dataset = groupby_dataset.rename({column:alias})

				# do a merge so that aggregations appear on the same row 
				if dimension_dataset is None: 
					dimension_dataset = groupby_dataset

					if dimensions_change_var is not None:  # avoid the case where there is no dimensions_change
						# make sure that the original dataset did not have agg_dim$names nor agg_dim$values in it.
						if "agg_dim$names" in dimension_dataset.columns:
							raise ValueError('Dataframe already contains column named "agg_dim$names"')

						if "agg_dim$values" in dimension_dataset.columns:
							raise ValueError('Dataframe already contains column named "agg_dim$values"')

						# convert the change-dimensions to concatenated strings
						aggregation_dimensions_names_set = set() # initialize
						groupby_set = set(groupby_var[i])

						for change_lst in dimensions_change_var:
							change_set = set(change_lst)

							# if the change-dimension set is wholly in the groupby_set
							if len(change_set.intersection(groupby_set)) == len(change_set):
								# There is a risk here if one dimension_change list is a subset of another.  It would match in the if-statement above.
								# This next bit works because: Assume that two dimension_change lists were configured such that one was a subset of the other.
								# Even if the current iteration of change_set is the subset of elements, the super-set will be processed on a later iteration and unioned to 
								#  the set "aggregation_dimensions_names_set".
								aggregation_dimensions_names_set = aggregation_dimensions_names_set.union(change_set)

						aggregation_dimensions_names_sorted_list = list(aggregation_dimensions_names_set)
						aggregation_dimensions_names_sorted_list.sort()

						aggregation_dimensions_names_sorted_list_str = ",".join(aggregation_dimensions_names_sorted_list)
						dimension_dataset = dimension_dataset.with_column(pl.lit(aggregation_dimensions_names_sorted_list_str).alias("agg_dim$names"))
						
						if len(aggregation_dimensions_names_sorted_list) > 0:
							dimension_dataset = dimension_dataset.with_column(pl.concat_str(dimension_dataset[aggregation_dimensions_names_sorted_list],",").alias("agg_dim$values"))
						else:
							dimension_dataset = dimension_dataset.with_column(pl.lit("").alias("agg_dim$values"))

				else:
					merge_cols = list(set(dimension_dataset.columns).intersection(groupby_dataset.columns))
					dimension_dataset = dimension_dataset.join(groupby_dataset, on=merge_cols, how="outer")

			# Do a concat because different levels of aggregation occured (can't merge)
			if return_dataset is None:
				return_dataset = dimension_dataset
			else:
				return_dataset = pl.concat([return_dataset, dimension_dataset], True, 'diagonal')

		# convert DataFrame back to pandas
		return_dataset = return_dataset.to_pandas()
		return return_dataset
