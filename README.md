# Description
The purpose of this Python module is to take a dataframe of detail-level data and return aggregated results.  We initially created it because SQL Server does not handle all aggregations (ex. median), and we wanted a workflow where detail-level data from SQL could be snapshotted when publishing a report but aggregated datasets would actually delievered in the end (since aggregated data helps to protect sensitive information).  This module performs the aggregation-portion of the workflow.

This module is built upon [Polars](https://github.com/pola-rs/polars), which is built upon [Rust](https://www.rust-lang.org/).  Using Polars/Rust allows us to compute aggregations incredibly efficiently compared to traditional Python multiprocessing due to reduced memory-requirements and compiled code; in short, it is "closer to the metal" of the machine, and we obtain processing times that are easily one-tenth of our previous Python pandas/multiprocessing-implementation.

To use our WWU_Aggregator module, simply install it using pip:

```
pip install wwuaggregator
```

# Usage
## Simple use-case
For this first example of usage, we will perform a sum of students in each college (College A, B, and C) over two years (2021 and 2022).

```python
import pandas as pd
from wwuaggregator import WWU_Aggregator

agg = WWU_Aggregator()

data = [{'college':'College A', 'year': '2021', 'students':1000 },
		{'college':'College A', 'year': '2022', 'students':1200 },
		{'college':'College B', 'year': '2021', 'students':500 },
		{'college':'College B', 'year': '2022', 'students':700 },
		{'college':'College C', 'year': '2021', 'students':1800 },
		{'college':'College C', 'year': '2022', 'students':1900 }]

df = pd.DataFrame(data)

results = agg.operations(
	[
		{"operation":"sum","column":"students"}
	]
) \
.dimensions_constant([['college']]) \
.dataframe(df) \
.execute()

print(results)
```


Result:

```
     college  students_sum
0  College A          2200
1  College B          1200
2  College C          3700
```


## Aggregating multiple sets of dimensions
In many of our reports, we want to have different sets of aggregation.  For example, we might want to get numbers by college for all years (like the example above), but we might also want the data broken out by college and year as well.
We can specify these groupings by supplying a list of lists to the *dimensions_constant*-parameter like this:  `.dimensions_constant([['college'], ['college','year']])`

Furthermore, we might also have a set of dimensions that want a report-user to be able to filter by in a dashboard we are creating.  We call these "change dimensions", and their presence expands the groupings to include the *dimensions_constant* as well as the Cartesian-product of *dimensions_constant* and *dimensions_change*.  This means that the user will be able to filter the dashboard by the given criteria and still obtain the aggregated results for the data specified by *dimensions_constant*.

In our example below, we sum the number of students by college and then also by college/year - while also adding department as a filter for the report.

```python
import pandas as pd
from wwuaggregator import WWU_Aggregator

agg = WWU_Aggregator()

data = [{'college':'College A', 'department':'Department 1', 'year': '2021', 'students':1000 },
		{'college':'College A', 'department':'Department 1', 'year': '2022', 'students':1200 },
		{'college':'College A', 'department':'Department 2', 'year': '2021', 'students':200 },
		{'college':'College A', 'department':'Department 2', 'year': '2022', 'students':300 },
		{'college':'College B', 'department':'Department 3', 'year': '2021', 'students':500 },
		{'college':'College B', 'department':'Department 3', 'year': '2022', 'students':700 },
		{'college':'College B', 'department':'Department 4', 'year': '2021', 'students':50 },
		{'college':'College B', 'department':'Department 4', 'year': '2022', 'students':75 },
		{'college':'College C', 'department':'Department 5', 'year': '2021', 'students':1800 },
		{'college':'College C', 'department':'Department 5', 'year': '2022', 'students':1900 },
		{'college':'College C', 'department':'Department 6', 'year': '2021', 'students':1100 },
		{'college':'College C', 'department':'Department 6', 'year': '2022', 'students':1250 }]

df = pd.DataFrame(data)

results = agg.operations(
	[
		{"operation":"sum","column":"students"}
	]
) \
.dimensions_constant([['college'], ['college','year']]) \
.dimensions_change([['department']]) \
.dataframe(df) \
.execute()

print(results)
```


Result:

```
      college    department  students_sum agg_dim$names agg_dim$values  year
0   College A  Department 1          2200    department   Department 1   NaN
1   College A  Department 2           500    department   Department 2   NaN
2   College B  Department 3          1200    department   Department 3   NaN
3   College B  Department 4           125    department   Department 4   NaN
4   College C  Department 5          3700    department   Department 5   NaN
5   College C  Department 6          2350    department   Department 6   NaN
6   College A  Department 1          1000    department   Department 1  2021
7   College A  Department 2           200    department   Department 2  2021
8   College A  Department 1          1200    department   Department 1  2022
9   College A  Department 2           300    department   Department 2  2022
10  College B  Department 3           500    department   Department 3  2021
11  College B  Department 4            50    department   Department 4  2021
12  College B  Department 3           700    department   Department 3  2022
13  College B  Department 4            75    department   Department 4  2022
14  College C  Department 5          1800    department   Department 5  2021
15  College C  Department 6          1100    department   Department 6  2021
16  College C  Department 5          1900    department   Department 5  2022
17  College C  Department 6          1250    department   Department 6  2022
18  College A           NaN          2700                                NaN
19  College B           NaN          1325                                NaN
20  College C           NaN          6050                                NaN
21  College A           NaN          1200                               2021
22  College A           NaN          1500                               2022
23  College B           NaN           550                               2021
24  College B           NaN           775                               2022
25  College C           NaN          2900                               2021
26  College C           NaN          3150                               2022
```


Note that entries of NaN signify that the data were not grouped by that dimension.  For example, line 18 was grouped only by college (since department and year are both NaN); however, line 6 was grouped by college, department, and year.
Additionally, this output contains new variables named *agg_dim$names* and *agg_dim$values*.  These columns can be used in the dashboarding tool to perform the filter (ex. "only show records where department = Department 1").

# Aggregation Operations
This tool offers several types of aggregation-operations.

## Simple statistics
This tool can perform the following simple statistics:

* count
* count_distinct
* max
* mean
* median
* min
* std _(standard deviation)_
* sum


## Percent of total
This tool can also perform percent of total for both categorical and numeric data.

### Categorical
Here is an example where we are answering the question "Of all the colleges in this grouping, what percentage was represented by College A?"

```python
import pandas as pd
from wwuaggregator import WWU_Aggregator

agg = WWU_Aggregator()

data = [{'college':'College A', 'department':'Department 1', 'year': '2021', 'students':1000 },
		{'college':'College A', 'department':'Department 1', 'year': '2022', 'students':1200 },
		{'college':'College A', 'department':'Department 2', 'year': '2021', 'students':200 },
		{'college':'College A', 'department':'Department 2', 'year': '2022', 'students':300 },
		{'college':'College B', 'department':'Department 3', 'year': '2021', 'students':500 },
		{'college':'College B', 'department':'Department 3', 'year': '2022', 'students':700 },
		{'college':'College B', 'department':'Department 4', 'year': '2021', 'students':50 },
		{'college':'College B', 'department':'Department 4', 'year': '2022', 'students':75 },
		{'college':'College C', 'department':'Department 5', 'year': '2021', 'students':1800 },
		{'college':'College C', 'department':'Department 5', 'year': '2022', 'students':1900 },
		{'college':'College C', 'department':'Department 6', 'year': '2021', 'students':1100 },
		{'college':'College C', 'department':'Department 6', 'year': '2022', 'students':1250 }]

df = pd.DataFrame(data)

results = agg.operations(
	[
		{"operation":"sum","column":"students"},
		{"operation":"percent_of_total_categorical","column":"college"},
	]
) \
.dimensions_constant([['college'], ['college','year']]) \
.dimensions_change([['department']]) \
.dataframe(df) \
.execute()

print(results)
```

Result:

```
      college    department  students_sum agg_dim$names agg_dim$values  college_percent_of_total_categorical  year
0   College A  Department 1          2200    department   Department 1                              1.000000   NaN
1   College A  Department 2           500    department   Department 2                              1.000000   NaN
2   College B  Department 3          1200    department   Department 3                              1.000000   NaN
3   College B  Department 4           125    department   Department 4                              1.000000   NaN
4   College C  Department 5          3700    department   Department 5                              1.000000   NaN
5   College C  Department 6          2350    department   Department 6                              1.000000   NaN
6   College A  Department 1          1000    department   Department 1                              1.000000  2021
7   College A  Department 2           200    department   Department 2                              1.000000  2021
8   College A  Department 1          1200    department   Department 1                              1.000000  2022
9   College A  Department 2           300    department   Department 2                              1.000000  2022
10  College B  Department 3           500    department   Department 3                              1.000000  2021
11  College B  Department 4            50    department   Department 4                              1.000000  2021
12  College B  Department 3           700    department   Department 3                              1.000000  2022
13  College B  Department 4            75    department   Department 4                              1.000000  2022
14  College C  Department 5          1800    department   Department 5                              1.000000  2021
15  College C  Department 6          1100    department   Department 6                              1.000000  2021
16  College C  Department 5          1900    department   Department 5                              1.000000  2022
17  College C  Department 6          1250    department   Department 6                              1.000000  2022
18  College A           NaN          2700                                                           0.333333   NaN
19  College B           NaN          1325                                                           0.333333   NaN
20  College C           NaN          6050                                                           0.333333   NaN
21  College A           NaN          1200                                                           0.333333  2021
22  College A           NaN          1500                                                           0.333333  2022
23  College B           NaN           550                                                           0.333333  2021
24  College B           NaN           775                                                           0.333333  2022
25  College C           NaN          2900                                                           0.333333  2021
26  College C           NaN          3150                                                           0.333333  2022
```

In this example, the top rows having 100% are due to the fact that it was grouping by department (and there is only one college for the department).  The bottom rows indicate that College A accounted for 33% of the grouping (which makes sense when it is four of the 12 records in the set).


### Numeric
Here is an example where we are answering the question "Of all the colleges in this grouping, what percentage of students was represented by College A?"

```python
import pandas as pd
from wwuaggregator import WWU_Aggregator

agg = WWU_Aggregator()

data = [{'college':'College A', 'department':'Department 1', 'year': '2021', 'students':1000 },
		{'college':'College A', 'department':'Department 1', 'year': '2022', 'students':1200 },
		{'college':'College A', 'department':'Department 2', 'year': '2021', 'students':200 },
		{'college':'College A', 'department':'Department 2', 'year': '2022', 'students':300 },
		{'college':'College B', 'department':'Department 3', 'year': '2021', 'students':500 },
		{'college':'College B', 'department':'Department 3', 'year': '2022', 'students':700 },
		{'college':'College B', 'department':'Department 4', 'year': '2021', 'students':50 },
		{'college':'College B', 'department':'Department 4', 'year': '2022', 'students':75 },
		{'college':'College C', 'department':'Department 5', 'year': '2021', 'students':1800 },
		{'college':'College C', 'department':'Department 5', 'year': '2022', 'students':1900 },
		{'college':'College C', 'department':'Department 6', 'year': '2021', 'students':1100 },
		{'college':'College C', 'department':'Department 6', 'year': '2022', 'students':1250 }]

df = pd.DataFrame(data)

results = agg.operations(
	[
		{"operation":"sum","column":"students"},
		{"operation":"percent_of_total_numeric","column":"students", "of_total":["college"]},
	]
) \
.dimensions_constant([['college'], ['college','year']]) \
.dimensions_change([['department']]) \
.dataframe(df) \
.execute()

print(results)
```

Result:

```
      college    department  students_sum agg_dim$names agg_dim$values  students_percent_of_total_numeric_college  year
0   College A  Department 1          2200    department   Department 1                                   0.814815   NaN
1   College A  Department 2           500    department   Department 2                                   0.185185   NaN
2   College B  Department 3          1200    department   Department 3                                   0.905660   NaN
3   College B  Department 4           125    department   Department 4                                   0.094340   NaN
4   College C  Department 5          3700    department   Department 5                                   0.611570   NaN
5   College C  Department 6          2350    department   Department 6                                   0.388430   NaN
6   College A  Department 1          1000    department   Department 1                                   0.370370  2021
7   College A  Department 2           200    department   Department 2                                   0.074074  2021
8   College A  Department 1          1200    department   Department 1                                   0.444444  2022
9   College A  Department 2           300    department   Department 2                                   0.111111  2022
10  College B  Department 3           500    department   Department 3                                   0.377358  2021
11  College B  Department 4            50    department   Department 4                                   0.037736  2021
12  College B  Department 3           700    department   Department 3                                   0.528302  2022
13  College B  Department 4            75    department   Department 4                                   0.056604  2022
14  College C  Department 5          1800    department   Department 5                                   0.297521  2021
15  College C  Department 6          1100    department   Department 6                                   0.181818  2021
16  College C  Department 5          1900    department   Department 5                                   0.314050  2022
17  College C  Department 6          1250    department   Department 6                                   0.206612  2022
18  College A           NaN          2700                                                                1.000000   NaN
19  College B           NaN          1325                                                                1.000000   NaN
20  College C           NaN          6050                                                                1.000000   NaN
21  College A           NaN          1200                                                                0.444444  2021
22  College A           NaN          1500                                                                0.555556  2022
23  College B           NaN           550                                                                0.415094  2021
24  College B           NaN           775                                                                0.584906  2022
25  College C           NaN          2900                                                                0.479339  2021
26  College C           NaN          3150                                                                0.520661  2022
```

In this example, lines 0 and 1 are aggregated by college and department.  Line 0's (Department 1's) value of 2200 is 81% of College A's (all departments) students.  Line 1's (Department 2's) value of 500 is 18.5% of College A's (all departments) students.

Now let's look at lines 6 through 17.  These lines are aggregated by college, departmnet, and year.  For line 6, the value of 1000 (College A/Department 1/Year 2021) is 37% of College A's students (all departments/all years).

For line 18, it is aggregated by college.  College A's value of 2700 students is 100% of College A's students.

For lines 21 and 22, it is aggregated by college and year.  Line 21's value of 1200 (College A/Year 2021) is 44% of College A's students (all years).


It is worth noting that we can also compute the percent of total for the entire set.  In our example above, we got 100% for each college on lines 18 - 20.  What if we wanted to know what percentage of students are associated with College A for the entire university?  To handle this scenario, we can pass a wildcard value of "*" for the "of_total"-value:


```python
import pandas as pd
from wwuaggregator import WWU_Aggregator

agg = WWU_Aggregator()

data = [{'college':'College A', 'department':'Department 1', 'year': '2021', 'students':1000 },
		{'college':'College A', 'department':'Department 1', 'year': '2022', 'students':1200 },
		{'college':'College A', 'department':'Department 2', 'year': '2021', 'students':200 },
		{'college':'College A', 'department':'Department 2', 'year': '2022', 'students':300 },
		{'college':'College B', 'department':'Department 3', 'year': '2021', 'students':500 },
		{'college':'College B', 'department':'Department 3', 'year': '2022', 'students':700 },
		{'college':'College B', 'department':'Department 4', 'year': '2021', 'students':50 },
		{'college':'College B', 'department':'Department 4', 'year': '2022', 'students':75 },
		{'college':'College C', 'department':'Department 5', 'year': '2021', 'students':1800 },
		{'college':'College C', 'department':'Department 5', 'year': '2022', 'students':1900 },
		{'college':'College C', 'department':'Department 6', 'year': '2021', 'students':1100 },
		{'college':'College C', 'department':'Department 6', 'year': '2022', 'students':1250 }]

df = pd.DataFrame(data)

results = agg.operations(
	[
		{"operation":"sum","column":"students"},
		{"operation":"percent_of_total_numeric","column":"students", "of_total":["*"]},
	]
) \
.dimensions_constant([['college'], ['college','year']]) \
.dimensions_change([['department']]) \
.dataframe(df) \
.execute()

print(results)
```

Result:

```
      college    department  students_sum agg_dim$names agg_dim$values  students_percent_of_total_numeric  year
0   College A  Department 1          2200    department   Department 1                           0.218362   NaN
1   College A  Department 2           500    department   Department 2                           0.049628   NaN
2   College B  Department 3          1200    department   Department 3                           0.119107   NaN
3   College B  Department 4           125    department   Department 4                           0.012407   NaN
4   College C  Department 5          3700    department   Department 5                           0.367246   NaN
5   College C  Department 6          2350    department   Department 6                           0.233251   NaN
6   College A  Department 1          1000    department   Department 1                           0.099256  2021
7   College A  Department 2           200    department   Department 2                           0.019851  2021
8   College A  Department 1          1200    department   Department 1                           0.119107  2022
9   College A  Department 2           300    department   Department 2                           0.029777  2022
10  College B  Department 3           500    department   Department 3                           0.049628  2021
11  College B  Department 4            50    department   Department 4                           0.004963  2021
12  College B  Department 3           700    department   Department 3                           0.069479  2022
13  College B  Department 4            75    department   Department 4                           0.007444  2022
14  College C  Department 5          1800    department   Department 5                           0.178660  2021
15  College C  Department 6          1100    department   Department 6                           0.109181  2021
16  College C  Department 5          1900    department   Department 5                           0.188586  2022
17  College C  Department 6          1250    department   Department 6                           0.124069  2022
18  College A           NaN          2700                                                        0.267990   NaN
19  College B           NaN          1325                                                        0.131514   NaN
20  College C           NaN          6050                                                        0.600496   NaN
21  College A           NaN          1200                                                        0.119107  2021
22  College A           NaN          1500                                                        0.148883  2022
23  College B           NaN           550                                                        0.054591  2021
24  College B           NaN           775                                                        0.076923  2022
25  College C           NaN          2900                                                        0.287841  2021
26  College C           NaN          3150                                                        0.312655  2022
```

For our particular question, we want to focus on lines 18 - 20, where it is aggregating by college only.  On line 18, the value of 2700 is 26.8% of the total students at the university for all departments and years (10,705).  Likewise, the other rows are being compared to the entirety of the set as well - for example, line 0's value of 2200 is also being compared to 10,705 and thus results in a calculation of 21.8%.  You will note that each grouping adds up to 100% (ex. lines 0 through 5, which are grouped by college and department).

## Calculations of the complement
In many of our scenarios, we'd like to compare one member of a set against its complement.  For example, perhaps we want to compare one professor's grades against the median-value of his or her peers' grades.  Or - using the dataset above - perhaps we want to know one college's student numbers against the mean of all the other colleges' student numbers.  In such scenarios, we want to pull the member of the set out of the distribution when computing the aggregation so that we are not comparing the member to itself.  We have a handful of operations that perform this kind of work in the aggregator:

* count_of_complement
* count_distinct_of_complement
* max_of_complement
* mean_of_complement
* median_of_complement
* min_of_complement
* std_of_complement
* sum_of_complement

Here is a college's number of students compared with the mean of the other colleges:

```python
import pandas as pd
from wwuaggregator import WWU_Aggregator

agg = WWU_Aggregator()

data = [{'college':'College A', 'department':'Department 1', 'year': '2021', 'students':1000 },
		{'college':'College A', 'department':'Department 1', 'year': '2022', 'students':1200 },
		{'college':'College A', 'department':'Department 2', 'year': '2021', 'students':200 },
		{'college':'College A', 'department':'Department 2', 'year': '2022', 'students':300 },
		{'college':'College B', 'department':'Department 3', 'year': '2021', 'students':500 },
		{'college':'College B', 'department':'Department 3', 'year': '2022', 'students':700 },
		{'college':'College B', 'department':'Department 4', 'year': '2021', 'students':50 },
		{'college':'College B', 'department':'Department 4', 'year': '2022', 'students':75 },
		{'college':'College C', 'department':'Department 5', 'year': '2021', 'students':1800 },
		{'college':'College C', 'department':'Department 5', 'year': '2022', 'students':1900 },
		{'college':'College C', 'department':'Department 6', 'year': '2021', 'students':1100 },
		{'college':'College C', 'department':'Department 6', 'year': '2022', 'students':1250 }]

df = pd.DataFrame(data)

results = agg.operations(
	[
		{"operation":"sum","column":"students"},
		{"operation":"mean_of_complement","column":"students","of_complement":["college"]},
	]
) \
.dimensions_constant([['college'], ['college','year']]) \
.dimensions_change([['department']]) \
.dataframe(df) \
.execute()

print(results)
```

Result:

```
      college    department  students_sum agg_dim$names agg_dim$values  year  students_mean_of_complement
0   College A  Department 1          2200    department   Department 1   NaN                          NaN
1   College A  Department 2           500    department   Department 2   NaN                          NaN
2   College B  Department 3          1200    department   Department 3   NaN                          NaN
3   College B  Department 4           125    department   Department 4   NaN                          NaN
4   College C  Department 5          3700    department   Department 5   NaN                          NaN
5   College C  Department 6          2350    department   Department 6   NaN                          NaN
6   College A  Department 1          1000    department   Department 1  2021                          NaN
7   College A  Department 2           200    department   Department 2  2021                          NaN
8   College A  Department 1          1200    department   Department 1  2022                          NaN
9   College A  Department 2           300    department   Department 2  2022                          NaN
10  College B  Department 3           500    department   Department 3  2021                          NaN
11  College B  Department 4            50    department   Department 4  2021                          NaN
12  College B  Department 3           700    department   Department 3  2022                          NaN
13  College B  Department 4            75    department   Department 4  2022                          NaN
14  College C  Department 5          1800    department   Department 5  2021                          NaN
15  College C  Department 6          1100    department   Department 6  2021                          NaN
16  College C  Department 5          1900    department   Department 5  2022                          NaN
17  College C  Department 6          1250    department   Department 6  2022                          NaN
18  College A           NaN          2700                                NaN                      921.875
19  College B           NaN          1325                                NaN                     1093.750
20  College C           NaN          6050                                NaN                      503.125
21  College A           NaN          1200                               2021                      862.500
22  College A           NaN          1500                               2022                      981.250
23  College B           NaN           550                               2021                     1025.000
24  College B           NaN           775                               2022                     1162.500
25  College C           NaN          2900                               2021                      437.500
26  College C           NaN          3150                               2022                      568.750
```

Let's take a look at line 18.  College A has a total of 2700 students, and the other colleges have an average of 921.875 students.  ...What?  How does that work?  Since B has 1325 and C has 6050, shouldn't have an average of something closer to 3600?
In this case, it was computing mean_of_complement at the department-level even though it was computing it for the college (i.e., it removed College A from the dataset and computed the mean of the remaining records - which happened to be at department-level aggregation).  Thus, (500 + 700 + 50 + 75 + 1800 + 1900 + 1100 + 1250) / 8 = 921.875.

In order to get a number closer to the 3600 that we expect, we will instead need to aggregate our data at the desired level (college) first and then perform the mean_of_complement.

```python
import pandas as pd
from wwuaggregator import WWU_Aggregator

data = [{'college':'College A', 'department':'Department 1', 'year': '2021', 'students':1000 },
		{'college':'College A', 'department':'Department 1', 'year': '2022', 'students':1200 },
		{'college':'College A', 'department':'Department 2', 'year': '2021', 'students':200 },
		{'college':'College A', 'department':'Department 2', 'year': '2022', 'students':300 },
		{'college':'College B', 'department':'Department 3', 'year': '2021', 'students':500 },
		{'college':'College B', 'department':'Department 3', 'year': '2022', 'students':700 },
		{'college':'College B', 'department':'Department 4', 'year': '2021', 'students':50 },
		{'college':'College B', 'department':'Department 4', 'year': '2022', 'students':75 },
		{'college':'College C', 'department':'Department 5', 'year': '2021', 'students':1800 },
		{'college':'College C', 'department':'Department 5', 'year': '2022', 'students':1900 },
		{'college':'College C', 'department':'Department 6', 'year': '2021', 'students':1100 },
		{'college':'College C', 'department':'Department 6', 'year': '2022', 'students':1250 }]

df = pd.DataFrame(data)

# sum things at the college-level
summation_agg = WWU_Aggregator()
summation = summation_agg.operations(
	[
		{"operation":"sum","column":"students"},
	]
) \
.dimensions_constant([['college']]) \
.dataframe(df) \
.execute()

# perform aggregation at the college-level of a college against its complement
agg = WWU_Aggregator()
results = agg.operations(
	[
		{"operation":"max","column":"students_sum"},
		{"operation":"mean_of_complement","column":"students_sum","of_complement":["college"]},
	]
) \
.dimensions_constant([['college']]) \
.dataframe(summation) \
.execute()

print(results)
```

Result:

```
     college  students_sum_max  students_sum_mean_of_complement
0  College A              2700                           3687.5
1  College B              1325                           4375.0
2  College C              6050                           2012.5
```
