import pandas as pd

def calculate_distance_matrix(dataset_path):

    df = pd.read_csv(r"C:\Users\DELL\Documents\GitHub\MapUp-Data-Assessment-F\datasets\dataset-3.csv")


    ids = sorted(set(df['id_start'].unique()) | set(df['id_end'].unique()))
    distance_matrix = pd.DataFrame(index=ids, columns=ids)


    distance_matrix.fillna(0, inplace=True)


    for index, row in df.iterrows():
        distance_matrix.loc[row['id_start'], row['id_end']] += row['distance']
        distance_matrix.loc[row['id_end'], row['id_start']] += row['distance']

    return distance_matrix


dataset_path = 'dataset-3.csv'
result_matrix = calculate_distance_matrix(dataset_path)
result_matrix.to_csv('output_of_first1_dataset.csv', index=False)


print(result_matrix)


from itertools import product

def unroll_distance_matrix(distance_matrix):

    id_combinations = list(product(distance_matrix.index, distance_matrix.columns))


    id_combinations = [(id_start, id_end) for id_start, id_end in id_combinations if id_start != id_end]


    unrolled_df = pd.DataFrame({
        'id_start': [id_start for id_start, _ in id_combinations],
        'id_end': [id_end for _, id_end in id_combinations],
        'distance': [distance_matrix.loc[id_start, id_end] for id_start, id_end in id_combinations]
    })

    return unrolled_df

output_file_path = 'output_of_first_dataset.csv'
df_distance_matrix = pd.read_csv(output_file_path)
result_unrolled = unroll_distance_matrix(df_distance_matrix)
result_unrolled.to_csv('output_of_second_dataset.csv', index=False)

print(result_unrolled)


def find_ids_within_ten_percentage_threshold(df, reference_value):

    reference_avg_distance = df[df['id_start'] == reference_value]['distance'].mean()


    lower_bound = reference_avg_distance - (0.1 * reference_avg_distance)
    upper_bound = reference_avg_distance + (0.1 * reference_avg_distance)


    filtered_df = df[(df['id_start'] != reference_value) & df['distance'].between(lower_bound, upper_bound, inclusive='both')]


    result_ids = sorted(filtered_df['id_start'].unique())

    return result_ids


output_file_path = 'output_of_second_dataset.csv'
df_second_dataset = pd.read_csv(output_file_path)


reference_value = 123

result_ids_within_threshold = find_ids_within_ten_percentage_threshold(df_second_dataset, reference_value)


print(result_ids_within_threshold)


def calculate_toll_rate(df):

    df['moto'] = df['distance'] * 0.8
    df['car'] = df['distance'] * 1.2
    df['rv'] = df['distance'] * 1.5
    df['bus'] = df['distance'] * 2.2
    df['truck'] = df['distance'] * 3.6

    return df


output_file_path = 'output_of_second_dataset.csv'
df_second_dataset = pd.read_csv(output_file_path)


df_with_toll_rates = calculate_toll_rate(df_second_dataset)


print(df_with_toll_rates)


from datetime import time


output_file_path = 'output_of_second_dataset.csv'
df_second_dataset = pd.read_csv(output_file_path)

def find_ids_within_ten_percentage_threshold(df, reference_value):

    reference_avg_distance = df[df['id_start'] == reference_value]['distance'].mean()


    lower_bound = reference_avg_distance - (0.1 * reference_avg_distance)
    upper_bound = reference_avg_distance + (0.1 * reference_avg_distance)


    filtered_df = df[(df['id_start'] != reference_value) & (df['distance'] >= lower_bound) & (df['distance'] <= upper_bound)]


    result_ids = sorted(filtered_df['id_start'].unique())

    return result_ids

def calculate_time_based_toll_rates(df):

    df_copy = df.copy()


    weekday_ranges = [(time(0, 0), time(10, 0)), (time(10, 0), time(18, 0)), (time(18, 0), time(23, 59, 59))]
    weekend_ranges = [(time(0, 0), time(23, 59, 59))]


    for day_range in [weekday_ranges, weekend_ranges]:
        for start_time, end_time in day_range:
            mask = (df_copy['start_time'] >= start_time) & (df_copy['end_time'] <= end_time)
            df_copy.loc[mask, ['moto', 'car', 'rv', 'bus', 'truck']] *= 0.8 if day_range == weekday_ranges else 0.7


    df_copy['start_day'] = df_copy['start_day'].apply(lambda x: pd.to_datetime(x).day_name())
    df_copy['end_day'] = df_copy['end_day'].apply(lambda x: pd.to_datetime(x).day_name())


    df_copy['start_time'] = pd.to_datetime(df_copy['start_time']).dt.time
    df_copy['end_time'] = pd.to_datetime(df_copy['end_time']).dt.time

    return df_copy


reference_value = 123

result_ids_within_threshold = find_ids_within_ten_percentage_threshold(df_second_dataset, reference_value)
print(result_ids_within_threshold)

df_with_time_based_toll_rates = calculate_time_based_toll_rates(df_second_dataset)
print(df_with_time_based_toll_rates)
