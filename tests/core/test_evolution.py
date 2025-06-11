from kroft.core.evolution import SchemaEvolutionController


def test_schema_evolver_skips_batches_and_limits_evolution():
    evolver = SchemaEvolutionController(
        evolution_interval=5,
        evolution_probability=1.0,  # Always trigger if on interval
        add_probability=1.0,        # Always choose 'add'
        max_additions=2,
        max_drops=1
    )

    assert evolver.should_evolve(5)
    assert evolver.choose_action() == "add"
    evolver.record_action("add")

    assert evolver.should_evolve(10)
    assert evolver.choose_action() == "add"
    evolver.record_action("add")

    # Now additions maxed out
    assert evolver.should_evolve(15)
    assert evolver.choose_action() == "drop"
    evolver.record_action("drop")

    # Now both maxed
    assert evolver.should_evolve(20)
    assert evolver.choose_action() == "none"