import pandas as pd

def generate_car_matrix(dataset_path):

    df = pd.read_csv(r"C:\Users\DELL\Documents\GitHub\MapUp-Data-Assessment-F\datasets\dataset-1.csv")


    car_matrix = df.pivot(index='id_1', columns='id_2', values='car')


    car_matrix = car_matrix.fillna(0)


    for col in car_matrix.columns:
        car_matrix.at[col, col] = 0

    return car_matrix


dataset_path = (r"C:\Users\DELL\Documents\GitHub\MapUp-Data-Assessment-F\datasets\dataset-1.csv")
result_matrix = generate_car_matrix(dataset_path)
result_matrix.to_csv('output_of_first_dataset.csv', index=False)

print(result_matrix)


def get_type_count(dataset_path):
    df = pd.read_csv(r"C:\Users\DELL\Documents\GitHub\MapUp-Data-Assessment-F\datasets\dataset-1.csv")


    df['car_type'] = pd.cut(df['car'], bins=[-float('inf'), 15, 25, float('inf')],
                            labels=['high', 'medium', 'low'], include_lowest=True)


    type_counts = df['car_type'].value_counts().to_dict()


    sorted_type_counts = dict(sorted(type_counts.items()))

    return sorted_type_counts



dataset_path = 'dataset-1.csv'
result = get_type_count(dataset_path)


print(result)


def get_bus_indexes(dataset_path):

    df = pd.read_csv(r"C:\Users\DELL\Documents\GitHub\MapUp-Data-Assessment-F\datasets\dataset-1.csv")


    bus_mean = df['bus'].mean()


    bus_indexes = df[df['bus'] > 2 * bus_mean].index.tolist()


    bus_indexes.sort()

    return bus_indexes


dataset_path = 'dataset-1.csv'
result = get_bus_indexes(dataset_path)


print("Result: ", result)


def filter_routes(df):

    route_averages = df.groupby('route')['truck'].mean()


    selected_routes = route_averages[route_averages > 7].index.tolist()


    selected_routes = sorted(selected_routes)

    return selected_routes


dataset_path = (r'C:\Users\DELL\Documents\GitHub\MapUp-Data-Assessment-F\datasets\dataset-1.csv')
df_dataset_1 = pd.read_csv(dataset_path)


result_routes = filter_routes(df_dataset_1)


print(result_routes)


def multiply_matrix(input_matrix):

    modified_matrix = input_matrix.copy()


    modified_matrix = modified_matrix.applymap(lambda x: x * 0.75 if x > 20 else x * 1.25)


    modified_matrix = modified_matrix.round(1)

    return modified_matrix



result_matrix = pd.read_csv('output_of_first_dataset.csv')
modified_result_matrix = multiply_matrix(result_matrix)


print(modified_result_matrix)


def verify_timestamp_completeness(df):

    df['start_timestamp'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'], errors='coerce')


    df['end_timestamp'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'], errors='coerce')


    df = df.dropna(subset=['start_timestamp', 'end_timestamp'])


    df['timestamp_diff'] = df['end_timestamp'] - df['start_timestamp']


    valid_timestamps = (
        (df['start_timestamp'].dt.time == pd.to_datetime('00:00:00').time()) &
        (df['end_timestamp'].dt.time == pd.to_datetime('23:59:59').time()) &
        (df['timestamp_diff'] == pd.to_timedelta('1 day')) &
        (df['start_timestamp'].dt.day_name().isin(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']))
    )


    if 'id' not in df.columns or 'id_2' not in df.columns:
        raise ValueError("Columns 'id' and 'id_2' are required in the DataFrame.")


    result_series = valid_timestamps.groupby(['id', 'id_2']).all()

    return result_series


df_dataset_2 = pd.read_csv(r"C:\Users\DELL\Documents\GitHub\MapUp-Data-Assessment-F\datasets\dataset-1.csv")
result = verify_timestamp_completeness(df_dataset_2)


print(result)
