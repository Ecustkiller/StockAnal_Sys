import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, JSON, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

# 读取配置
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///data/stock_analyzer.db')
USE_DATABASE = os.getenv('USE_DATABASE', 'True').lower() == 'true'

# 创建引擎
engine = create_engine(DATABASE_URL, echo=False)
Base = declarative_base()


# ==================== 原有模型 ====================

class StockInfo(Base):
    __tablename__ = 'stock_info'

    id = Column(Integer, primary_key=True)
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50))
    market_type = Column(String(5))
    industry = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def to_dict(self):
        return {
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'market_type': self.market_type,
            'industry': self.industry,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None
        }


class AnalysisResult(Base):
    __tablename__ = 'analysis_results'

    id = Column(Integer, primary_key=True)
    stock_code = Column(String(10), nullable=False, index=True)
    market_type = Column(String(5))
    analysis_date = Column(DateTime, default=datetime.now)
    score = Column(Float)
    recommendation = Column(String(100))
    technical_data = Column(JSON)
    fundamental_data = Column(JSON)
    capital_flow_data = Column(JSON)
    ai_analysis = Column(Text)

    def to_dict(self):
        return {
            'stock_code': self.stock_code,
            'market_type': self.market_type,
            'analysis_date': self.analysis_date.strftime('%Y-%m-%d %H:%M:%S') if self.analysis_date else None,
            'score': self.score,
            'recommendation': self.recommendation,
            'technical_data': self.technical_data,
            'fundamental_data': self.fundamental_data,
            'capital_flow_data': self.capital_flow_data,
            'ai_analysis': self.ai_analysis
        }


class Portfolio(Base):
    """旧版投资组合（兼容保留）"""
    __tablename__ = 'portfolios'

    id = Column(Integer, primary_key=True)
    user_id = Column(String(50), nullable=False, index=True)
    name = Column(String(100))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    stocks = Column(JSON)

    def to_dict(self):
        return {
            'id': self.id,
            'user_id': self.user_id,
            'name': self.name,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None,
            'stocks': self.stocks
        }


# ==================== 自选股管理 ====================

class WatchlistGroup(Base):
    """自选股分组"""
    __tablename__ = 'watchlist_groups'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    description = Column(String(200), default='')
    color = Column(String(20), default='#4e73df')  # 分组颜色标识
    sort_order = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    stocks = relationship('WatchlistStock', back_populates='group', cascade='all, delete-orphan')

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'color': self.color,
            'sort_order': self.sort_order,
            'stock_count': len(self.stocks) if self.stocks else 0,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None,
        }


class WatchlistStock(Base):
    """自选股条目"""
    __tablename__ = 'watchlist_stocks'

    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey('watchlist_groups.id'), nullable=False, index=True)
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50), default='')
    market_type = Column(String(5), default='A')
    cost_price = Column(Float, default=0)  # 成本价（可选）
    target_price = Column(Float, default=0)  # 目标价（可选）
    stop_loss_price = Column(Float, default=0)  # 止损价（可选）
    notes = Column(Text, default='')  # 备注
    sort_order = Column(Integer, default=0)
    added_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    group = relationship('WatchlistGroup', back_populates='stocks')

    def to_dict(self):
        return {
            'id': self.id,
            'group_id': self.group_id,
            'group_name': self.group.name if self.group else '',
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'market_type': self.market_type,
            'cost_price': self.cost_price,
            'target_price': self.target_price,
            'stop_loss_price': self.stop_loss_price,
            'notes': self.notes,
            'sort_order': self.sort_order,
            'added_at': self.added_at.strftime('%Y-%m-%d %H:%M:%S') if self.added_at else None,
        }


# ==================== 投资组合管理 ====================

class PortfolioAccount(Base):
    """投资组合账户"""
    __tablename__ = 'portfolio_accounts'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(String(200), default='')
    initial_capital = Column(Float, default=0)  # 初始资金
    cash_balance = Column(Float, default=0)  # 现金余额
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    holdings = relationship('PortfolioHolding', back_populates='account', cascade='all, delete-orphan')
    transactions = relationship('PortfolioTransaction', back_populates='account', cascade='all, delete-orphan')

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'initial_capital': self.initial_capital,
            'cash_balance': self.cash_balance,
            'holding_count': len(self.holdings) if self.holdings else 0,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None,
        }


class PortfolioHolding(Base):
    """持仓记录"""
    __tablename__ = 'portfolio_holdings'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50), default='')
    market_type = Column(String(5), default='A')
    quantity = Column(Integer, default=0)  # 持仓数量
    avg_cost = Column(Float, default=0)  # 平均成本
    current_price = Column(Float, default=0)  # 当前价格（缓存）
    price_updated_at = Column(DateTime)  # 价格更新时间
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    account = relationship('PortfolioAccount', back_populates='holdings')

    def to_dict(self):
        market_value = self.quantity * self.current_price if self.current_price else 0
        cost_value = self.quantity * self.avg_cost if self.avg_cost else 0
        profit = market_value - cost_value
        profit_pct = (profit / cost_value * 100) if cost_value > 0 else 0
        return {
            'id': self.id,
            'account_id': self.account_id,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'market_type': self.market_type,
            'quantity': self.quantity,
            'avg_cost': self.avg_cost,
            'current_price': self.current_price,
            'market_value': round(market_value, 2),
            'cost_value': round(cost_value, 2),
            'profit': round(profit, 2),
            'profit_pct': round(profit_pct, 2),
            'price_updated_at': self.price_updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.price_updated_at else None,
        }


class PortfolioTransaction(Base):
    """交易记录"""
    __tablename__ = 'portfolio_transactions'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50), default='')
    market_type = Column(String(5), default='A')
    action = Column(String(10), nullable=False)  # buy / sell
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    commission = Column(Float, default=0)  # 手续费
    tax = Column(Float, default=0)  # 印花税
    notes = Column(Text, default='')
    trade_date = Column(DateTime, default=datetime.now)
    created_at = Column(DateTime, default=datetime.now)

    account = relationship('PortfolioAccount', back_populates='transactions')

    def to_dict(self):
        total = self.quantity * self.price
        return {
            'id': self.id,
            'account_id': self.account_id,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'market_type': self.market_type,
            'action': self.action,
            'quantity': self.quantity,
            'price': self.price,
            'total': round(total, 2),
            'commission': self.commission,
            'tax': self.tax,
            'notes': self.notes,
            'trade_date': self.trade_date.strftime('%Y-%m-%d %H:%M:%S') if self.trade_date else None,
        }


# ==================== 风险预警 ====================

class AlertRule(Base):
    """预警规则"""
    __tablename__ = 'alert_rules'

    id = Column(Integer, primary_key=True)
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50), default='')
    rule_type = Column(String(30), nullable=False)  # price_above/price_below/rsi_overbought/rsi_oversold/volume_surge/macd_cross/ma_break/stop_loss
    condition_value = Column(Float, default=0)  # 条件阈值
    is_active = Column(Boolean, default=True)
    is_triggered = Column(Boolean, default=False)
    last_triggered_at = Column(DateTime)
    description = Column(String(200), default='')
    created_at = Column(DateTime, default=datetime.now)

    def to_dict(self):
        return {
            'id': self.id,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'rule_type': self.rule_type,
            'condition_value': self.condition_value,
            'is_active': self.is_active,
            'is_triggered': self.is_triggered,
            'last_triggered_at': self.last_triggered_at.strftime('%Y-%m-%d %H:%M:%S') if self.last_triggered_at else None,
            'description': self.description,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
        }


class AlertLog(Base):
    """预警日志"""
    __tablename__ = 'alert_logs'

    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, ForeignKey('alert_rules.id'), nullable=True)
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50), default='')
    alert_type = Column(String(30), nullable=False)
    alert_level = Column(String(10), default='info')  # info/warning/danger
    message = Column(Text, nullable=False)
    current_value = Column(Float)
    threshold_value = Column(Float)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now)

    def to_dict(self):
        return {
            'id': self.id,
            'rule_id': self.rule_id,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'alert_type': self.alert_type,
            'alert_level': self.alert_level,
            'message': self.message,
            'current_value': self.current_value,
            'threshold_value': self.threshold_value,
            'is_read': self.is_read,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
        }


# ==================== 每日市场简报 ====================

class DailyBrief(Base):
    """每日市场简报"""
    __tablename__ = 'daily_briefs'

    id = Column(Integer, primary_key=True)
    brief_date = Column(String(10), nullable=False, unique=True, index=True)  # YYYY-MM-DD
    market_summary = Column(JSON)  # 大盘总结
    sector_rotation = Column(JSON)  # 板块轮动
    capital_flow = Column(JSON)  # 资金流向
    limit_up_analysis = Column(JSON)  # 涨停分析
    watchlist_alerts = Column(JSON)  # 自选股异动
    full_report = Column(Text)  # 完整简报文本
    created_at = Column(DateTime, default=datetime.now)

    def to_dict(self):
        return {
            'id': self.id,
            'brief_date': self.brief_date,
            'market_summary': self.market_summary,
            'sector_rotation': self.sector_rotation,
            'capital_flow': self.capital_flow,
            'limit_up_analysis': self.limit_up_analysis,
            'watchlist_alerts': self.watchlist_alerts,
            'full_report': self.full_report,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
        }


# 创建会话工厂
Session = sessionmaker(bind=engine)


# 初始化数据库
def init_db():
    """创建所有表"""
    Base.metadata.create_all(engine)


# 获取数据库会话
def get_session():
    return Session()


# 始终初始化数据库（确保表结构存在）
init_db()