# Function to get valid input with specified ranges
def get_valid_input(prompt, default_value, min_value=None, max_value=None, input_type=int):
    while True:
        try:
            value = input(f"{prompt} (default: {default_value}): ").strip()
            if value == "":
                value = default_value
            elif input_type == bool:
                if value.lower() in ['true', 't', 'yes', 'y', '1']:
                    value = True
                elif value.lower() in ['false', 'f', 'no', 'n', '0']:
                    value = False
                else:
                    raise ValueError("Invalid boolean value")
            else:
                value = input_type(value)
            if min_value is not None and value < min_value:
                print(f"Please enter a value bigger than {min_value}.")
            elif max_value is not None and value > max_value:
                print(f"Please enter a value smaller than {max_value}.")
            else:
                return value
        except ValueError:
            print("Please enter a valid value.")