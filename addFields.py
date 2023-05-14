import yaml
import sys

FIELDS_TO_ESCALATE = ['worker-station', 'worker-trip', 'worker-weather', 'distributor']


def isInField(s):
    for x in FIELDS_TO_ESCALATE:
        if s.startswith(x):
            return True
    return False


def addClients(services: dict, stations, trips, weather, distributors):
    keys = services.keys()
    copyKeys = []
    for k in keys:
        if isInField(k):
            copyKeys.append(k)
    for k in copyKeys:
        del services[k]
    for i in range(stations):
        clientId = i + 1
        services[f'worker-station{clientId}'] = {
            'container_name': f'worker-station{clientId}',
            'image': 'client:latest',
            'build': {'context': './workers/worker-stations'},
            'environment':
                [f'id={clientId}'],
            'networks': ['bikers'],
            'depends_on': {'rabbit': {'condition': 'service_healthy'}},
            'volumes': ['./workers/worker-stations/main.go/:/app/main.go']
        }
    for i in range(trips):
        clientId = i + 1
        services[f'worker-trip{clientId}'] = {
            'container_name': f'worker-trip{clientId}',
            'image': 'client:latest',
            'build': {'context': './workers/worker-trips'},
            'environment':
                [f'id={clientId}'],
            'networks': ['bikers'],
            'depends_on': {'rabbit': {'condition': 'service_healthy'}},
            'volumes': ['./workers/worker-trips/main.go/:/app/main.go']
        }
    for i in range(weather):
        clientId = i + 1
        services[f'worker-weather{clientId}'] = {
            'container_name': f'worker-weather{clientId}',
            'image': 'client:latest',
            'build': {'context': './workers/worker-weather'},
            'environment':
                [f'id={clientId}'],
            'networks': ['bikers'],
            'depends_on': {'rabbit': {'condition': 'service_healthy'}},
            'volumes': ['./workers/worker-weather/main.go/:/app/main.go']
        }
    for i in range(distributors):
        clientId = i + 1
        services[f'distributor{clientId}'] = {
            'container_name': f'distributor{clientId}',
            'image': 'client:latest',
            'build': {'context': './distributor'},
            'environment':
                [f'id={clientId}'],
            'networks': ['bikers'],
            'depends_on': {'rabbit': {'condition': 'service_healthy'}},
            'volumes': ['./distributor/main.go/:/app/main.go']
        }


def isValidParam(p):
    return p and p.isdigit()


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(f'missing parameters, required 4 received: {len(sys.argv) - 1}')
        exit(1)
    _, stations, trips, weather, distributors = sys.argv
    if not (isValidParam(stations) and isValidParam(trips) and isValidParam(weather) and isValidParam(distributors)):
        print('wrong amount of something, all should be number')
        exit(1)
    info = {}
    with open('docker-compose.yml', 'r') as f:
        info = yaml.load(f, Loader=yaml.FullLoader)
    addClients(info['services'], int(stations), int(trips), int(weather), int(distributors))
    with open('docker-compose.yml', 'w') as f:
        yaml.dump(info, f)
