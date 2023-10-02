"""
This file contains functions that are used to prepare the data for analysis.
"""

import pandas as pd
import datetime

def add_fulldate_column(df: pd.DataFrame, date_column: str = "Date", time_column: str = "Heure") -> pd.DataFrame:
    """
    Adds a column to the dataframe with the full date of the match.

    Args:
        df (pd.DataFrame): The dataframe to add the column to.
        date_column (str, optional): The name of the column with the date. Defaults to "Date".
        time_column (str, optional): The name of the column with the time. Defaults to "Heure".

    Returns:
        pd.DataFrame: The dataframe with the new column.
    """

    df['FullDate'] = pd.to_datetime(df[date_column] + " " + df[time_column], format='%Y-%m-%d %H:%M')
    return df

def get_finished_matches(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a dataframe with only the finished matches.

    Args:
        df (pd.DataFrame): The dataframe to filter.

    Returns:
        pd.DataFrame: The filtered dataframe.
    """

    # Note: We remove 3 hours because the matches are not updated immediately.
    df = df[df['FullDate'] < datetime.datetime.now() - datetime.timedelta(hours=3)]
    return df

def add_goals_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column to the dataframe with the goals scored by the home team and the away team.

    Args:
        df (pd.DataFrame): The dataframe to add the column to.

    Returns:
        pd.DataFrame: The dataframe with the new column.
    """

    df[['domicile_but', 'exterieur_but']] = df['Score'].str.split('–', expand=True).astype(int)
    return df

def merge_statistics_to_match_schedule(df_schedule: pd.DataFrame, df_statistics: pd.DataFrame) -> pd.DataFrame:
    """
    Merges teams general statistics to the match schedule.

    Args:
        df_schedule (pd.DataFrame): The match schedule.
        df_statistics (pd.DataFrame): The teams general statistics.
    
    Returns:
        pd.DataFrame: The merged dataframe.
    """
    merged_df = df_schedule.merge(
        df_statistics, left_on="Domicile", right_on="Équipe", suffixes=("_dom", "_ext")
    ).merge(
        df_statistics, left_on="Extérieur", right_on="Équipe", suffixes=("_dom", "_ext")
    )

    return merged_df

def get_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a series with the feature for the team.

    Args:
        df (pd.DataFrame): The dataframe to get the feature from.

    Returns:
        pd.DataFrame: The DataFrame with the feature.
    """

    # TODO
    # Note: this is a simplication of the function, it should be more complex

    X = df.drop(["FullDate", "Domicile", "Extérieur", "Équipe_dom", "5 derniers_dom", "Affluence_dom",
                    "Meilleur buteur de l'équipe_dom", "Gardien de but_dom", "Notes_dom", "Équipe_ext", "5 derniers_ext",
                    "Affluence_ext", "Meilleur buteur de l'équipe_ext", "Gardien de but_ext", "Notes_ext", "domicile_but", "exterieur_but"], axis=1)
    return X

def get_home_target(df: pd.DataFrame) -> pd.Series:
    """
    Returns a series with the home target (number of home team goals).

    Args:
        df (pd.DataFrame): The dataframe to get the target from.

    Returns:
        pd.DataFrame: The DataFrame with the target.
    """
   
    return df["domicile_but"]

def get_ext_target(df: pd.DataFrame) -> pd.Series:
    """
    Returns a series with the home target (number of exterior team goals).

    Args:
        df (pd.DataFrame): The dataframe to get the target from.

    Returns:
        pd.DataFrame: The DataFrame with the target.
    """
   
    return df["exterieur_but"]

def create_confrontation_stats_df(home_team: str, away_team: str, df_statistics: pd.DataFrame):
    """
    Creates a dataframe with the statistics of the two teams.

    Args:
        home_team (str): The home team. Note: case sensitive.
        away_team (str): The exterior team. Note: case sensitive.
        df_statistics (pd.DataFrame): The dataframe with the statistics.

    Returns:
        pd.DataFrame: The dataframe with the statistics of the two teams.
    """
    X_home_team = df_statistics[df_statistics['Équipe'] == home_team]
    X_away_team = df_statistics[df_statistics['Équipe'] == away_team]

    return X_home_team.merge(
        X_away_team, how="cross", suffixes=("_dom", "_ext")
    )





