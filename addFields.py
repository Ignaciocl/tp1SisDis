import yaml
import sys

FIELDS_TO_ESCALATE = ['worker-station', 'worker-trip', 'worker-weather', 'distributor', 'calculator']


def isInField(s):
    for x in FIELDS_TO_ESCALATE:
        if s.startswith(x):
            return True
    return False


def setEnvVar(s: dict, name: str, amount: int):
    env = s.get('environment', [])
    if s.get('volumes', False):
        s.pop('volumes')
    for k in env:
        if k.startswith(name):
            env.remove(k)
    env.append(f'{name}={amount}')
    s['environment'] = env


def addClients(services: dict, stations, trips, weather, distributors, calculators):
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
            'build': {'context': './workers/worker-stations'},
            'environment':
                [f'id={clientId}', f'distributors={distributors}'],
            'networks': ['bikers'],
            'depends_on': {'rabbit': {'condition': 'service_healthy'}},
        }
    for i in range(trips):
        clientId = i + 1
        services[f'worker-trip{clientId}'] = {
            'container_name': f'worker-trip{clientId}',
            'build': {'context': './workers/worker-trips'},
            'environment':
                [f'id={clientId}', f'distributors={distributors}'],
            'networks': ['bikers'],
            'depends_on': {'rabbit': {'condition': 'service_healthy'}},
        }
    for i in range(weather):
        clientId = i + 1
        services[f'worker-weather{clientId}'] = {
            'container_name': f'worker-weather{clientId}',
            'build': {'context': './workers/worker-weather'},
            'environment':
                [f'id={clientId}', f'distributors={distributors}'],
            'networks': ['bikers'],
            'depends_on': {'rabbit': {'condition': 'service_healthy'}},
        }
    for i in range(distributors):
        clientId = i + 1
        services[f'distributor{clientId}'] = {
            'container_name': f'distributor{clientId}',
            'build': {'context': './distributor'},
            'environment':
                [f'id={clientId}'],
            'networks': ['bikers'],
            'depends_on': {'rabbit': {'condition': 'service_healthy'}},
        }
    for i in range(calculators):
        clientId = i + 1
        services[f'calculator{clientId}'] = {
            'container_name': f'calculator{clientId}',
            'build': {'context': './calculator'},
            'environment':
                [f'id={clientId}'],
            'networks': ['bikers'],
            'depends_on': {'rabbit': {'condition': 'service_healthy'}},
        }
    setEnvVar(services['accumulator-montreal'], 'calculators', calculators)
    setEnvVar(services['joiner-montreal'], 'calculators', calculators)
    setEnvVar(services['joiner-montreal'], 'amountStationsWorkers', stations)
    setEnvVar(services['joiner-montreal'], 'amountTripsWorkers', trips)
    setEnvVar(services['joiner-stations'], 'amountStationsWorkers', stations)
    setEnvVar(services['joiner-stations'], 'amountTripsWorkers', trips)
    setEnvVar(services['joiner-weather'], 'amountWeatherWorkers', weather)
    setEnvVar(services['joiner-weather'], 'amountTripsWorkers', trips)
    setEnvVar(services['server'], 'distributors', distributors)


def isValidParam(p):
    return p and p.isdigit()


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print(f'missing parameters, required 5 received: {len(sys.argv) - 1}')
        exit(1)
    _, stations, trips, weather, distributors, calculator = sys.argv
    if not (isValidParam(stations) and isValidParam(trips) and isValidParam(weather) and isValidParam(distributors)):
        print('wrong amount of something, all should be number')
        exit(1)
    info = {}
    with open('docker-compose.yml', 'r') as f:
        info = yaml.load(f, Loader=yaml.FullLoader)
    addClients(info['services'], int(stations), int(trips), int(weather), int(distributors), int(calculator))
    with open('docker-compose.yml', 'w') as f:
        yaml.dump(info, f)
