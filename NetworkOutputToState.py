import sys

input_file = sys.argv[1]
output_file = sys.argv[2]

vertices = set()
state_nodes = {}
edges = set()

with open(input_file, 'r') as f:

    # First, build a dictionary of vertex IDs starting at 1 and their names
    for line in f:
        ''' Line format is [FromNode],[ToNode], weight
            where [FromNode] and [ToNode] have format current_node|past0.past1...
        '''
        from_node, to_node, weight = line.strip().split(',')
        physical_name_from = from_node.split('|')[0]
        physical_name_to = to_node.split('|')[0]

        vertices.add(physical_name_to)
        vertices.add(physical_name_from)

    keys = list(range(1, len(vertices)+1))
    vertices = dict(zip(vertices, keys))

    # Now, go through and get unique ID's for state nodes, use dict to get their
    # matching physical node
    f.seek(0)
    unique_ID = 1
    for line in f:
        from_node, to_node, weight = line.strip().split(',')
        physical_name_from = from_node.split('|')[0]
        physical_name_to = to_node.split('|')[0]

        physical_ID_from = vertices[physical_name_from]
        physical_ID_to = vertices[physical_name_to]

        if from_node not in state_nodes:
            state_nodes[from_node] = (unique_ID, physical_ID_from)
            unique_ID += 1
        if to_node not in state_nodes:
            state_nodes[to_node] = (unique_ID, physical_ID_to)
            unique_ID += 1

    # Finally, get edge weights
    f.seek(0)
    for line in f:
        from_node, to_node, weight = line.strip().split(',')
        edges.add((state_nodes[from_node][0], state_nodes[to_node][0], weight))


with open(output_file, 'w') as f:
    f.write("*Vertices %d \n"%len(vertices))
    for vertex in vertices:
        f.write(str(vertices[vertex])+" "+vertex+"\n")
    f.write("*States\n")
    for node in state_nodes:
        f.write(str(state_nodes[node][0])+" "+str(state_nodes[node][1])+" "+str(node)+"\n")
    f.write("*Links\n")
    for edge in edges:
        f.write('%d %d %d \n'%(int(edge[0]), int(edge[1]), int(edge[2])))
