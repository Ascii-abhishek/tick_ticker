from app.services.contract_service import ContractService, round_to_nearest_step


def test_round_to_nearest_step() -> None:
    assert round_to_nearest_step(22482, 50) == 22500
    assert round_to_nearest_step(22474, 50) == 22450


def test_strikes_around_atm() -> None:
    strikes = ContractService().strikes_around_atm(spot_price=22482, step=50, window=2)

    assert strikes == (22400.0, 22450.0, 22500.0, 22550.0, 22600.0)

