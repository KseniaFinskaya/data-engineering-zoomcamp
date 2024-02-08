
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    # Filter out records with zero passenger_count
    print('Rows with zero passengers:', data[data['passenger_count'] <= 0].sum())
    data = data[data['passenger_count'] > 0]

    # Filter out records with zero trip_distance
    print('Rows with zero trip distance:', data[data['trip_distance'] <= 0].sum())
    data = data[data['trip_distance'] > 0]    
    # create and convert date column from existing one
    data['lpep_pickup_date']=data['lpep_pickup_datetime'].dt.date
    # rename columns to snake case properly
    data = data.rename(columns={"VendorID": "vendor_id", 
    "RatecodeID": "ratecode_id",
    "PULocationID":"pu_location_id",
    "DOLocationID":"do_location_id"})
    return data



@test
def test_passenger_count(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum()==0, 'There rides with zero passengers'
    
@test
def test_trip_distance_value(output, *args) -> None:
    assert output['trip_distance'].isin([0]).sum()==0, 'There rides with zero distance'

@test
def test_vendor_column_exists(output, *args) -> None:
    assert output.columns.isin(['vendor_id']).any(), 'vendor_id column doesn\'t exist'
