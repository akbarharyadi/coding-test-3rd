"""
Fund API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from app.db.session import get_db
from app.models.fund import Fund
from app.models.transaction import CapitalCall, Distribution, Adjustment
from app.schemas.fund import Fund as FundSchema, FundCreate, FundUpdate, FundMetrics
from app.schemas.transaction import (
    CapitalCall as CapitalCallSchema,
    Distribution as DistributionSchema,
    Adjustment as AdjustmentSchema,
    TransactionList
)
from app.services.metrics_calculator import MetricsCalculator

router = APIRouter()


@router.get("/", response_model=List[FundSchema])
async def list_funds(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """List all funds"""
    funds = db.query(Fund).offset(skip).limit(limit).all()
    
    # Add metrics to each fund
    calculator = MetricsCalculator(db)
    result = []
    
    for fund in funds:
        fund_dict = FundSchema.model_validate(fund).model_dump()
        metrics = calculator.calculate_all_metrics(fund.id)
        fund_dict["metrics"] = FundMetrics(**metrics)
        result.append(FundSchema(**fund_dict))
    
    return result


@router.post("/", response_model=FundSchema)
async def create_fund(fund: FundCreate, db: Session = Depends(get_db)):
    """Create a new fund"""
    db_fund = Fund(**fund.model_dump())
    db.add(db_fund)
    db.commit()
    db.refresh(db_fund)
    return db_fund


@router.get("/compare")
async def compare_funds(
    fund_ids: str = Query(..., description="Comma-separated fund IDs to compare"),
    db: Session = Depends(get_db)
):
    """Compare multiple funds side-by-side"""
    # Parse fund IDs
    try:
        ids = [int(id.strip()) for id in fund_ids.split(',')]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid fund_ids format. Use comma-separated integers.")

    if len(ids) < 2:
        raise HTTPException(status_code=400, detail="At least 2 funds are required for comparison")

    if len(ids) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 funds can be compared at once")

    # Fetch all funds
    funds = db.query(Fund).filter(Fund.id.in_(ids)).all()

    if len(funds) != len(ids):
        found_ids = {f.id for f in funds}
        missing_ids = set(ids) - found_ids
        raise HTTPException(
            status_code=404,
            detail=f"Funds not found: {', '.join(map(str, missing_ids))}"
        )

    # Calculate metrics for each fund
    calculator = MetricsCalculator(db)
    comparison_data = []

    for fund in funds:
        metrics = calculator.calculate_all_metrics(fund.id)

        # Get capital calls and distributions count
        capital_calls_count = db.query(CapitalCall).filter(CapitalCall.fund_id == fund.id).count()
        distributions_count = db.query(Distribution).filter(Distribution.fund_id == fund.id).count()

        comparison_data.append({
            'fund_id': fund.id,
            'fund_name': fund.name,
            'gp_name': fund.gp_name,
            'vintage_year': fund.vintage_year,
            'metrics': metrics,
            'capital_calls_count': capital_calls_count,
            'distributions_count': distributions_count,
        })

    # Calculate rankings
    comparison_data_with_ranks = []
    metric_keys = ['dpi', 'tvpi', 'irr', 'moic', 'rvpi']

    for metric_key in metric_keys:
        # Get values for this metric (filter out None values)
        values = [(i, item['metrics'].get(metric_key))
                  for i, item in enumerate(comparison_data)
                  if item['metrics'].get(metric_key) is not None]

        if values:
            # Sort by value (descending) and assign ranks
            values.sort(key=lambda x: x[1], reverse=True)
            ranks = {idx: rank + 1 for rank, (idx, _) in enumerate(values)}

            # Add ranks to comparison data
            for i, item in enumerate(comparison_data):
                if 'rankings' not in item:
                    item['rankings'] = {}
                item['rankings'][metric_key] = ranks.get(i)

    return {
        'funds': comparison_data,
        'comparison_date': db.query(Fund).first().created_at.isoformat() if funds else None,
        'total_compared': len(comparison_data)
    }


@router.get("/{fund_id}", response_model=FundSchema)
async def get_fund(fund_id: int, db: Session = Depends(get_db)):
    """Get fund details"""
    fund = db.query(Fund).filter(Fund.id == fund_id).first()

    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")

    # Add metrics
    calculator = MetricsCalculator(db)
    metrics = calculator.calculate_all_metrics(fund_id)

    fund_dict = FundSchema.model_validate(fund).model_dump()
    fund_dict["metrics"] = FundMetrics(**metrics)

    return FundSchema(**fund_dict)


@router.put("/{fund_id}", response_model=FundSchema)
async def update_fund(
    fund_id: int,
    fund_update: FundUpdate,
    db: Session = Depends(get_db)
):
    """Update fund details"""
    fund = db.query(Fund).filter(Fund.id == fund_id).first()
    
    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")
    
    update_data = fund_update.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(fund, key, value)
    
    db.commit()
    db.refresh(fund)
    return fund


@router.delete("/{fund_id}")
async def delete_fund(fund_id: int, db: Session = Depends(get_db)):
    """Delete a fund"""
    fund = db.query(Fund).filter(Fund.id == fund_id).first()
    
    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")
    
    db.delete(fund)
    db.commit()
    
    return {"message": "Fund deleted successfully"}


@router.get("/{fund_id}/transactions", response_model=TransactionList)
async def get_fund_transactions(
    fund_id: int,
    transaction_type: str = Query(..., regex="^(capital_calls|distributions|adjustments)$"),
    page: int = 1,
    limit: int = 50,
    db: Session = Depends(get_db)
):
    """Get fund transactions"""
    # Verify fund exists
    fund = db.query(Fund).filter(Fund.id == fund_id).first()
    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")
    
    # Query based on transaction type
    if transaction_type == "capital_calls":
        query = db.query(CapitalCall).filter(CapitalCall.fund_id == fund_id)
        model = CapitalCallSchema
    elif transaction_type == "distributions":
        query = db.query(Distribution).filter(Distribution.fund_id == fund_id)
        model = DistributionSchema
    else:  # adjustments
        query = db.query(Adjustment).filter(Adjustment.fund_id == fund_id)
        model = AdjustmentSchema
    
    # Get total count
    total = query.count()
    
    # Paginate
    skip = (page - 1) * limit
    items = query.offset(skip).limit(limit).all()
    
    # Calculate total pages
    pages = (total + limit - 1) // limit
    
    return TransactionList(
        items=[model.model_validate(item) for item in items],
        total=total,
        page=page,
        pages=pages
    )


@router.get("/{fund_id}/metrics", response_model=FundMetrics)
async def get_fund_metrics(fund_id: int, db: Session = Depends(get_db)):
    """Get fund metrics"""
    fund = db.query(Fund).filter(Fund.id == fund_id).first()
    
    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")
    
    calculator = MetricsCalculator(db)
    metrics = calculator.calculate_all_metrics(fund_id)
    
    return FundMetrics(**metrics)


@router.get("/{fund_id}/historical_data")
async def get_fund_historical_data(fund_id: int, db: Session = Depends(get_db)):
    """Get historical fund data for charts"""
    fund = db.query(Fund).filter(Fund.id == fund_id).first()
    if not fund:
        raise HTTPException(status_code=404, detail="Fund not found")

    # Get all capital calls for the fund
    capital_calls = db.query(CapitalCall).filter(CapitalCall.fund_id == fund_id).all()
    # Get all distributions for the fund
    distributions = db.query(Distribution).filter(Distribution.fund_id == fund_id).all()

    # Create timeline data
    timeline_data = []

    # Add capital calls to timeline
    for call in capital_calls:
        timeline_data.append({
            'date': call.call_date.isoformat(),
            'type': 'capital_call',
            'amount': float(call.amount),
            'description': call.description or call.call_type or 'Capital Call'
        })

    # Add distributions to timeline
    for dist in distributions:
        timeline_data.append({
            'date': dist.distribution_date.isoformat(),
            'type': 'distribution',
            'amount': float(dist.amount),
            'description': dist.description or dist.distribution_type or 'Distribution'
        })

    # Sort by date
    timeline_data.sort(key=lambda x: x['date'])

    # Create cumulative data
    cumulative_data = []
    cumulative_paid_in = 0
    cumulative_distributed = 0

    for event in timeline_data:
        if event['type'] == 'capital_call':
            cumulative_paid_in += event['amount']
        elif event['type'] == 'distribution':
            cumulative_distributed += event['amount']

        cumulative_data.append({
            'date': event['date'],
            'cumulative_paid_in': cumulative_paid_in,
            'cumulative_distributed': cumulative_distributed,
            'net_position': cumulative_distributed - cumulative_paid_in
        })

    # Calculate DPI and TVPI over time
    cumulative_metrics = []
    for event in cumulative_data:
        dpi = event['cumulative_paid_in'] > 0 and event['cumulative_distributed'] / event['cumulative_paid_in'] or 0
        tvpi = event['cumulative_paid_in'] > 0 and (event['cumulative_distributed'] + (event['cumulative_paid_in'] - event['cumulative_distributed'])) / event['cumulative_paid_in'] or 0
        cumulative_metrics.append({
            'date': event['date'],
            'dpi': round(dpi, 4),
            'tvpi': round(tvpi, 4)
        })

    return {
        'fund_id': fund_id,
        'fund_name': fund.name,
        'timeline_data': timeline_data,
        'cumulative_data': cumulative_data,
        'cumulative_metrics': cumulative_metrics
    }
