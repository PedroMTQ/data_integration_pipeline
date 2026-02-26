from cleanco import basename


# we usually go more in depth with this, especially if dealing with multiple geolocations
def normalize_company_name(company_name: str) -> str:
    return basename(company_name)


if __name__ == "__main__":
    print(normalize_company_name("discord Inc."))
