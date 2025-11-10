import pandas as pd
import osmnx as ox
import networkx as nx
import pickle
import os
from tqdm import tqdm
from multiprocessing import Pool

G_worker = None
G_nodes_worker = None

def init_worker():
    global G_worker, G_nodes_worker
    G_worker = ox.load_graphml('seoul_bike_network.graphml')
    G_worker = add_weights(G_worker)
    G_nodes_worker = dict(G_worker.nodes(data=True))  # dict로 변환

def add_weights(G):
    for u, v, data in G.edges(data=True):
        base_length = data.get('length', 100)
        highway = data.get('highway', '')
        if isinstance(highway, list):
            highway = highway[0]
        cycleway = data.get('cycleway', '')
        
        if cycleway in ['track', 'lane']:
            penalty = 0.5
        elif 'cycleway' in str(highway) or highway == 'path':
            penalty = 0.7
        elif highway in ['residential', 'living_street']:
            penalty = 0.9
        elif highway in ['primary', 'secondary', 'tertiary']:
            penalty = 1.5
        elif highway in ['trunk', 'motorway']:
            penalty = 3.0
        else:
            penalty = 1.0
        
        data['bike_weight'] = base_length * penalty
    return G

def astar_heuristic(u, v):
    global G_nodes_worker
    node_u = G_nodes_worker[u]
    node_v = G_nodes_worker[v]
    
    # lat, lon 거리 (간단한 유클리드)
    lat1, lon1 = node_u['y'], node_u['x']
    lat2, lon2 = node_v['y'], node_v['x']
    
    return ((lat1 - lat2)**2 + (lon1 - lon2)**2)**0.5

def calc_route_fast(orig_node, dest_node):
    global G_worker
    try:
        route_nodes = nx.astar_path(
            G_worker, 
            orig_node, 
            dest_node,
            heuristic=astar_heuristic,
            weight='bike_weight'
        )
        
        # 거리 계산 (MultiDiGraph 대응)
        length = 0
        for i in range(len(route_nodes)-1):
            edge_data = G_worker[route_nodes[i]][route_nodes[i+1]]
            if isinstance(edge_data, dict) and 0 in edge_data:
                length += edge_data[0].get('length', 0)
            else:
                length += edge_data.get('length', 0)
        
        return {'nodes': route_nodes, 'length': length}
    except:
        return None

def process_single_worker(row):
    route = calc_route_fast(row['node_a'], row['node_b'])
    return (row['station_a'], row['station_b'], route)

def main():
    print("Loading network for node mapping...")
    G = ox.load_graphml('seoul_bike_network.graphml')

    print("Loading OD data...")
    od_data = pd.read_pickle('od_with_location.pkl')

    print("Pre-calculating nearest nodes for unique stations...")
    
    stations_a = od_data[['station_a', 'lon_a', 'lat_a']].rename(
        columns={'station_a': 'id', 'lon_a': 'lon', 'lat_a': 'lat'}
    )
    stations_b = od_data[['station_b', 'lon_b', 'lat_b']].rename(
        columns={'station_b': 'id', 'lon_b': 'lon', 'lat_b': 'lat'}
    )
    
    unique_stations = pd.concat([stations_a, stations_b]).drop_duplicates(subset=['id']).set_index('id')
    
    nodes = ox.nearest_nodes(G, X=unique_stations['lon'].values, Y=unique_stations['lat'].values)
    unique_stations['node'] = nodes
    
    station_node_map = unique_stations['node'].to_dict()

    od_data['node_a'] = od_data['station_a'].map(station_node_map)
    od_data['node_b'] = od_data['station_b'].map(station_node_map)
    
    od_data = od_data.dropna(subset=['node_a', 'node_b'])
    od_data['node_a'] = od_data['node_a'].astype(int)
    od_data['node_b'] = od_data['node_b'].astype(int)

    del G

    args_list = [row for _, row in od_data.iterrows()]
    
    num_processes = os.cpu_count()
    print(f"\nCalculating {len(args_list):,} routes with {num_processes} processes (using A*)...")

    route_dict = {}

    with Pool(processes=num_processes, initializer=init_worker) as pool:
        results = list(tqdm(
            pool.imap(process_single_worker, args_list, chunksize=100),
            total=len(args_list)
        ))

    for station_a, station_b, route in results:
        route_dict[(station_a, station_b)] = route

    with open('route_cache.pkl', 'wb') as f:
        pickle.dump(route_dict, f)

    success = sum(1 for v in route_dict.values() if v is not None)
    print(f"\nSuccess: {success:,} / {len(route_dict):,}")
    print("Saved to route_cache.pkl")

if __name__ == '__main__':
    main()