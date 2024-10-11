import yaml
import getpass


def yaml_input():
    LoginPass = getpass.getpass("Enter your password: ")
    yaml_file = 'db_config.yaml'

    # Read the current data from the YAML file
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)

    # Write the data to a YAML file
    with open(yaml_file, 'w') as file:
        yaml.dump(data, file, default_flow_style=False)

    # Add the password to the existing 'mysql' section
    if 'mysql' in data:
        data['mysql']['password'] = LoginPass
    else:
        print("The 'mysql' section does not exist in the YAML file.")

    # Write the updated data back to the YAML file
    with open(yaml_file, 'w') as file:
        yaml.dump(data, file, default_flow_style=False)

    return 1
