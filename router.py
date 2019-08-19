from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

import json
import sys

with open(sys.argv[1], 'r') as f:
	query = json.load(f)

base = tuple(query['base'])
drones = query['drones']

if query['cells'] is None:
	print json.dumps([[]] * len(drones))
	sys.exit(0)

cell_rewards = {}
for t in query['cells']:
	cell_rewards[(t[0], t[1])] = t[2]
battery = max([drone['battery'] for drone in drones])

cell_list = [base] + cell_rewards.keys()
idx_to_cell = {i: cell for i, cell in enumerate(cell_list)}

distance_matrix = []
for a in cell_list:
	row = []
	for b in cell_list:
		dx = abs(a[0] - b[0])
		dy = abs(a[1] - b[1])
		row.append(dx + dy)
	distance_matrix.append(row)

depot = 0
manager = pywrapcp.RoutingIndexManager(len(distance_matrix), len(drones), depot)
routing = pywrapcp.RoutingModel(manager)

def distance_callback(from_index, to_index):
	from_node = manager.IndexToNode(from_index)
	to_node = manager.IndexToNode(to_index)
	return distance_matrix[from_node][to_node]

transit_callback_index = routing.RegisterTransitCallback(distance_callback)
routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
routing.AddDimension(
	transit_callback_index,
	0,
	battery,
	True,
	'Distance'
)
for node in range(1, len(cell_list)):
	cell = idx_to_cell[node]
	routing.AddDisjunction([manager.NodeToIndex(node)], cell_rewards[cell])

search_parameters = pywrapcp.DefaultRoutingSearchParameters()
search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.BEST_INSERTION)
search_parameters.time_limit.seconds = 30
solution = routing.SolveWithParameters(search_parameters)
if solution and False:
	max_route_distance = 0
	for vehicle_id in range(len(drones)):
		index = routing.Start(vehicle_id)
		plan_output = 'Route for vehicle {}:\n'.format(vehicle_id)
		route_distance = 0
		while not routing.IsEnd(index):
			plan_output += ' {} -> '.format(manager.IndexToNode(index))
			previous_index = index
			index = solution.Value(routing.NextVar(index))
			route_distance += routing.GetArcCostForVehicle(previous_index, index, vehicle_id)
		plan_output += '{}\n'.format(manager.IndexToNode(index))
		plan_output += 'Distance of the route: {}m\n'.format(route_distance)
		print(plan_output)
		max_route_distance = max(route_distance, max_route_distance)
	print('Maximum of the route distances: {}m'.format(max_route_distance))

routes = []
for vehicle_id in range(len(drones)):
	route = []
	index = routing.Start(vehicle_id)
	route.append(cell_list[manager.IndexToNode(index)])
	while not routing.IsEnd(index):
		index = solution.Value(routing.NextVar(index))
		route.append(cell_list[manager.IndexToNode(index)])
	routes.append(route)
print json.dumps(routes)
