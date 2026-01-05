import pandas as pd

class DataForge:
    def __init__(self):
        self.forge_df = pd.DataFrame()

    def insert_data(self, series: pd.Series, column_name: str):
        """
        Insert data into the Dataframe
        Index is always Date
        If the index in the new pandas series doesn't exist in the forge_df, it will be added and
        with NaN values will be created for other columns.
        """
        # Ensure the index is datetime with format YYYY-MM-DD
        series.index = pd.to_datetime(series.index).normalize()
        series = series.rename(column_name)
        self.forge_df = pd.concat([self.forge_df, series], axis=1)
        self.forge_df = self.forge_df.sort_index(ascending=True)

        return self.forge_df