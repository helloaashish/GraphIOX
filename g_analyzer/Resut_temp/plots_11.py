
import matplotlib.pyplot as plt
import seaborn as sns 
import networkx as nx
import numpy as np
import pandas as pd
from matplotlib.colors import ListedColormap


def hmap_red(mp=None, df=None, size_bins=[], size_labels=[], degree_bins=[], degree_labels = []):
    # df= inter.compute()
    if mp:
        df= df[df['mount_point']==mp]
    else:
        mp = "All Mount Points"
    # size_bins, size_labels, degree_bins, degree_labels = binned_category()
    df['size_category'] = pd.cut(df['size'], bins=size_bins, labels=size_labels, right=False)
    # df['deg_category'] = pd.cut(df['deg_caller'], bins=degree_bins, labels=degree_labels, right=False)
    df['IF_val'] = df['interference'].astype(float)
    all_combinations = pd.MultiIndex.from_product([df['size_category'].unique(), df['deg_caller'].unique()], names=['size_category', 'deg_caller'])
    merged = pd.merge(df, pd.DataFrame(index=all_combinations).reset_index(), on=['size_category', 'deg_caller'], how='left')
    grouped = merged.groupby(['size_category', 'deg_caller']).agg({'IF_val': 'mean'}).reset_index()
    pivot_table = grouped.pivot(index='size_category', columns='deg_caller', values='IF_val').fillna(-1)
    
        # Loop through each column and apply the 1 / val comparison with deg_caller
    comparison = pivot_table.apply(lambda x: np.where(1 / x < x.name, 0, 1)) 

    # Replace infinite values due to division by zero if needed (optional)
    comparison.replace([np.inf, -np.inf], 0, inplace=True)

    # Create heatmap using seaborn
    plt.figure(figsize=(6, 6))
    cmap = ListedColormap(['green', 'red'])
    sns.heatmap(comparison, annot=False, cmap=cmap,cbar = False, fmt='.1f', linewidths=.5, vmin=0, vmax=1)
    plt.title(f'Average Interference Heatmap {mp}')
    plt.xlabel('Degrees')
    plt.ylabel('Sizes')

       # Add a legend outside the heatmap
    legend_labels = ['Non-Interfering', 'Interfering']
    legend_colors = [cmap(0), cmap(1)]
    legend_elements = [
        plt.Line2D([0], [0], marker='s', color=color, markersize=10, label=label, linestyle='') 
        for color, label in zip(legend_colors, legend_labels)
    ]
    plt.legend(
        handles=legend_elements,
        loc='center left',
        bbox_to_anchor=(1, 0.5),
        title='Legend',
        frameon=False
    )
    plt.show()

def hmap_red_deepspeedlow(mp=None, df=None, size_bins=[], size_labels=[], degree_bins=[], degree_labels = []):
    # df= inter.compute()
    if mp:
        df= df[df['mount_point']==mp]
    else:
        mp = "All Mount Points"
    # size_bins, size_labels, degree_bins, degree_labels = binned_category()
    df['size_category'] = pd.cut(df['size'], bins=size_bins, labels=size_labels, right=False)
    # df['deg_category'] = pd.cut(df['deg_caller'], bins=degree_bins, labels=degree_labels, right=False)
    df['IF_val'] = df['interference'].astype(float)
    all_combinations = pd.MultiIndex.from_product([df['size_category'].unique(), df['deg_caller'].unique()], names=['size_category', 'deg_caller'])
    merged = pd.merge(df, pd.DataFrame(index=all_combinations).reset_index(), on=['size_category', 'deg_caller'], how='left')
    grouped = merged.groupby(['size_category', 'deg_caller']).agg({'IF_val': 'mean'}).reset_index()
    pivot_table = grouped.pivot(index='size_category', columns='deg_caller', values='IF_val').fillna(-1)
    
        # Loop through each column and apply the 1 / val comparison with deg_caller
    comparison = pivot_table.apply(lambda x: np.where(1 / x < x.name, 0, 1)) 

    # Replace infinite values due to division by zero if needed (optional)
    comparison.replace([np.inf, -np.inf], 0, inplace=True)

    # Create heatmap using seaborn
    plt.figure(figsize=(6, 6))
    cmap = ListedColormap(['green', 'red'])
    sns.heatmap(comparison, annot=False, cmap=cmap,cbar = False, fmt='.1f', linewidths=.5, vmin=0, vmax=1)
    plt.title(f'Average Interference Heatmap {mp}')
    plt.xlabel('Degrees')
    plt.ylabel('Sizes')

       # Add a legend outside the heatmap
    legend_labels = ['Non-Interfering', 'Interfering']
    legend_colors = [cmap(0), cmap(1)]
    legend_elements = [
        plt.Line2D([0], [0], marker='s', color=color, markersize=10, label=label, linestyle='') 
        for color, label in zip(legend_colors, legend_labels)
    ]
    plt.legend(
        handles=legend_elements,
        loc='center left',
        bbox_to_anchor=(1, 0.5),
        title='Legend',
        frameon=False
    )
    plt.show()