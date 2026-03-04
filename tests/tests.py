def test_model_mapper_maps_known_sources():
    from data_integration_pipeline.core.data_processing.model_mapper import ModelMapper
    from data_integration_pipeline.core.data_processing.data_models.data_sources import BusinessEntityRegistryRecord

    assert ModelMapper.get_data_model('bronze/business_entity_registry/file.csv') is BusinessEntityRegistryRecord
