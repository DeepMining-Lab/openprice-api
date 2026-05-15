from fastapi import APIRouter, HTTPException
from app.config import get_config
from app.registry import SUPPORTED_ASSETS, all_relative_paths, resolve_path
from app.csv_adapter import inspect
from app.schemas import DatasetsResponse, DatasetFile, SchemaResponse, ColumnMapping, Warning

router = APIRouter(prefix="/v1")


@router.get(
    "/datasets",
    response_model=DatasetsResponse,
    summary="List registered dataset files",
    description=(
        "Returns every CSV file registered in the source registry, with an `exists` flag "
        "indicating whether the file is present on disk. "
        "Useful for verifying which datasets are available before querying prices."
    ),
    tags=["Assets & Datasets"],
)
def list_datasets():
    cfg = get_config()
    files = []
    seen = set()
    for asset in SUPPORTED_ASSETS:
        for role, rel in all_relative_paths(asset):
            key = (asset, rel)
            if key in seen:
                continue
            seen.add(key)
            path = resolve_path(rel)
            files.append(DatasetFile(
                asset=asset,
                path=rel,
                exists=path.exists(),
                role=role,
            ))
    return DatasetsResponse(datasets_root=str(cfg.paths.datasets_path), files=files)


@router.get(
    "/datasets/schema",
    response_model=SchemaResponse,
    summary="Inspect CSV column schema for an asset",
    description=(
        "For each CSV file registered for the given asset, returns the raw column names found "
        "in the file header and the canonical mapping applied by the schema adapter "
        "(e.g. `price_usdc_per_link` → `price_usd`, `pool_tvl_at_block` → `tvl_usd`). "
        "Files that do not yet exist on disk are listed with a `file_not_found` warning."
    ),
    tags=["Assets & Datasets"],
)
def dataset_schema(asset: str):
    asset = asset.upper()
    if asset not in SUPPORTED_ASSETS:
        raise HTTPException(status_code=404, detail=f"Asset '{asset}' not supported.")
    mappings = []
    seen = set()
    for role, rel in all_relative_paths(asset):
        if rel in seen:
            continue
        seen.add(rel)
        path = resolve_path(rel)
        if not path.exists():
            mappings.append(ColumnMapping(
                file=rel,
                raw_columns=[],
                canonical_mapping={},
                warnings=[Warning(code="file_not_found",
                                  message=f"File does not exist: {rel}",
                                  severity="error")],
            ))
            continue
        schema = inspect(path)
        mappings.append(ColumnMapping(
            file=rel,
            raw_columns=schema.raw_columns,
            canonical_mapping=schema.mapping,
        ))
    return SchemaResponse(asset=asset, files=mappings)
