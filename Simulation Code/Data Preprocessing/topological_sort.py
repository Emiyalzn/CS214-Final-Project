import json


def read_json(json_path):
    json_dict = json.load(open(json_path, 'r'))
    return json_dict


def topo_sort(order_list, task_list):
    # init
    in_deg, out, edges, ans = {}, {}, {}, {}
    for task in task_list:
        in_deg[task] = 0
        out[task] = 0
        edges[task] = []
    for edge in order_list:
        in_deg[edge[1]] += 1
        edges[edge[0]].append(edge[1])
    # sort
    stage = 1
    while 0 in out.values():
        ans[stage] = []
        for task, deg in in_deg.items():
            if deg == 0 and not out[task]:
                ans[stage].append(task)
                out[task] = 1
        for task in ans[stage]:
            for dst in edges[task]:
                in_deg[dst] -= 1
        stage += 1
    return ans


if __name__ == "__main__":
    json_dict = read_json('./ToyData_dict.json')
    for key, value in json_dict.items():
        stage_order = topo_sort(value['order'],
                                list(value['Execution Time'].keys()))
        json_dict[key]["stage"] = stage_order
    json.dump(json_dict, open("sorted_ToyData_dict.json", 'w'))
