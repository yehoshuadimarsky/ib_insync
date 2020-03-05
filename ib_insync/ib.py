import asyncio
import datetime
import logging
import time
from contextlib import suppress
from typing import Awaitable, Iterator, List, Union

from eventkit import Event

import ib_insync.util as util
from ib_insync.client import Client
from ib_insync.contract import Contract
from ib_insync.objects import (
    AccountValue, BarDataList, BarList, BracketOrder, ContractDescription,
    ContractDetails, DepthMktDataDescription, Execution, ExecutionFilter, Fill,
    HistogramData, HistoricalNews, NewsArticle, NewsBulletin, NewsProvider,
    NewsTick, OptionChain, OptionComputation, OrderState, PnL, PnLSingle,
    PortfolioItem, Position, PriceIncrement, RealTimeBarList, ScanDataList,
    ScannerSubscription, TagValue, TradeLogEntry)
from ib_insync.order import LimitOrder, Order, OrderStatus, StopOrder, Trade
from ib_insync.ticker import Ticker
from ib_insync.wrapper import Wrapper

__all__ = ['IB']


class IB:
    """
    Provides both a blocking and an asynchronous interface
    to the IB API, using asyncio networking and event loop.

    The IB class offers direct access to the current state, such as
    orders, executions, positions, tickers etc. This state is
    automatically kept in sync with the TWS/IBG application.

    This class has most request methods of EClient, with the
    same names and parameters (except for the reqId parameter
    which is not needed anymore).
    Request methods that return a result come in two versions:

    * Blocking: Will block until complete and return the result.
      The current state will be kept updated while the request is ongoing;

    * Asynchronous: All methods that have the "Async" postfix.
      Implemented as coroutines or methods that return a Future and
      intended for advanced users.

    **The One Rule:**

    While some of the request methods are blocking from the perspective
    of the user, the framework will still keep spinning in the background
    and handle all messages received from TWS/IBG. It is important to
    not block the framework from doing its work. If, for example,
    the user code spends much time in a calculation, or uses time.sleep()
    with a long delay, the framework will stop spinning, messages
    accumulate and things may go awry.

    The one rule when working with the IB class is therefore that

    **user code may not block for too long**.

    To be clear, the IB request methods are okay to use and do not
    count towards the user operation time, no matter how long the
    request takes to finish.

    So what is "too long"? That depends on the situation. If, for example,
    the timestamp of tick data is to remain accurate within a millisecond,
    then the user code must not spend longer than a millisecond. If, on
    the other extreme, there is very little incoming data and there
    is no desire for accurate timestamps, then the user code can block
    for hours.

    If a user operation takes a long time then it can be farmed out
    to a different process.
    Alternatively the operation can be made such that it periodically
    calls IB.sleep(0); This will let the framework handle any pending
    work and return when finished. The operation should be aware
    that the current state may have been updated during the sleep(0) call.

    For introducing a delay, never use time.sleep() but use
    :meth:`.sleep` instead.

    Attributes:
        RequestTimeout (float): Timeout (in seconds) to wait for a
          blocking request to finish before raising ``asyncio.TimeoutError``.
          The default value of 0 will wait indefinitely.
          Note: This timeout is not used for the ``*Async`` methods.


    Events:
        * ``connectedEvent`` ():
          Is emitted after connecting and synchronzing with TWS/gateway.

        * ``disconnectedEvent`` ():
          Is emitted after disconnecting from TWS/gateway.

        * ``updateEvent`` ():
          Is emitted after a network packet has been handeled.

        * ``pendingTickersEvent`` (tickers: Set[:class:`.Ticker`]):
          Emits the set of tickers that have been updated during the last
          update and for which there are new ticks, tickByTicks or domTicks.

        * ``barUpdateEvent`` (bars: :class:`.BarDataList`,
          hasNewBar: bool): Emits the bar list that has been updated in
          real time. If a new bar has been added then hasNewBar is True,
          when the last bar has changed it is False.

        * ``newOrderEvent`` (trade: :class:`.Trade`):
          Emits a newly placed trade.

        * ``orderModifyEvent`` (trade: :class:`.Trade`):
          Emits when order is modified.

        * ``cancelOrderEvent`` (trade: :class:`.Trade`):
          Emits a trade directly after requesting for it to be cancelled.

        * ``openOrderEvent`` (trade: :class:`.Trade`):
          Emits the trade with open order.

        * ``orderStatusEvent`` (trade: :class:`.Trade`):
          Emits the changed order status of the ongoing trade.

        * ``execDetailsEvent`` (trade: :class:`.Trade`, fill: :class:`.Fill`):
          Emits the fill together with the ongoing trade it belongs to.

        * ``commissionReportEvent`` (trade: :class:`.Trade`,
          fill: :class:`.Fill`, report: :class:`.CommissionReport`):
          The commission report is emitted after the fill that it belongs to.

        * ``updatePortfolioEvent`` (item: :class:`.PortfolioItem`):
          A portfolio item has changed.

        * ``positionEvent`` (position: :class:`.Position`):
          A position has changed.

        * ``accountValueEvent`` (value: :class:`.AccountValue`):
          An account value has changed.

        * ``accountSummaryEvent`` (value: :class:`.AccountValue`):
          An account value has changed.

        * ``pnlEvent`` (entry: :class:`.PnL`):
          A profit- and loss entry is updated.

        * ``pnlSingleEvent`` (entry: :class:`.PnLSingle`):
          A profit- and loss entry for a single position is updated.

        * ``tickNewsEvent`` (news: :class:`.NewsTick`):
          Emit a new news headline.

        * ``newsBulletinEvent`` (bulletin: :class:`.NewsBulletin`):
          Emit a new news bulletin.

        * ``scannerDataEvent`` (data: :class:`.ScanDataList`):
          Emit data from a scanner subscription.

        * ``errorEvent`` (reqId: int, errorCode: int, errorString: str,
          contract: :class:`.Contract`):
          Emits the reqId/orderId and TWS error code and string (see
          https://interactivebrokers.github.io/tws-api/message_codes.html)
          together with the contract the error applies to (or None if no
          contract applies).

        * ``timeoutEvent`` (idlePeriod: float):
          Is emitted if no data is received for longer than the timeout period
          specified with :meth:`.setTimeout`. The value emitted is the period
          in seconds since the last update.

        Note that it is not advisable to place new requests inside an event
        handler as it may lead to too much recursion.
    """

    events = (
        'connectedEvent', 'disconnectedEvent', 'updateEvent',
        'pendingTickersEvent', 'barUpdateEvent',
        'newOrderEvent', 'orderModifyEvent', 'cancelOrderEvent',
        'openOrderEvent', 'orderStatusEvent',
        'execDetailsEvent', 'commissionReportEvent',
        'updatePortfolioEvent', 'positionEvent', 'accountValueEvent',
        'accountSummaryEvent', 'pnlEvent', 'pnlSingleEvent',
        'scannerDataEvent', 'tickNewsEvent', 'newsBulletinEvent',
        'errorEvent', 'timeoutEvent')

    RequestTimeout = 0

    def __init__(self):
        self._createEvents()
        self.wrapper = Wrapper(self)
        self.client = Client(self.wrapper)
        self.client.apiEnd += self.disconnectedEvent
        self._logger = logging.getLogger('ib_insync.ib')

    def _createEvents(self):
        self.connectedEvent = Event('connectedEvent')
        self.disconnectedEvent = Event('disconnectedEvent')
        self.updateEvent = Event('updateEvent')
        self.pendingTickersEvent = Event('pendingTickersEvent')
        self.barUpdateEvent = Event('barUpdateEvent')
        self.newOrderEvent = Event('newOrderEvent')
        self.orderModifyEvent = Event('orderModifyEvent')
        self.cancelOrderEvent = Event('cancelOrderEvent')
        self.openOrderEvent = Event('openOrderEvent')
        self.orderStatusEvent = Event('orderStatusEvent')
        self.execDetailsEvent = Event('execDetailsEvent')
        self.commissionReportEvent = Event('commissionReportEvent')
        self.updatePortfolioEvent = Event('updatePortfolioEvent')
        self.positionEvent = Event('positionEvent')
        self.accountValueEvent = Event('accountValueEvent')
        self.accountSummaryEvent = Event('accountSummaryEvent')
        self.pnlEvent = Event('pnlEvent')
        self.pnlSingleEvent = Event('pnlSingleEvent')
        self.scannerDataEvent = Event('scannerDataEvent')
        self.tickNewsEvent = Event('tickNewsEvent')
        self.newsBulletinEvent = Event('newsBulletinEvent')
        self.errorEvent = Event('errorEvent')
        self.timeoutEvent = Event('timeoutEvent')

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.disconnect()

    def __repr__(self):
        conn = (f'connected to {self.client.host}:'
                f'{self.client.port} clientId={self.client.clientId}' if
                self.client.isConnected() else 'not connected')
        return f'<{self.__class__.__qualname__} {conn}>'

    def connect(
            self, host: str = '127.0.0.1', port: int = 7497,
            clientId: int = 1, timeout: float = 2, readonly: bool = False):
        """
        Connect to a running TWS or IB gateway application.
        After the connection is made the client is fully synchronized
        and ready to serve requests.

        This method is blocking.

        Args:
            host: Host name or IP address.
            port: Port number.
            clientId: ID number to use for this client; must be unique per
                connection. Setting clientId=0 will automatically merge manual
                TWS trading with this client.
            timeout: If establishing the connection takes longer than
                ``timeout`` seconds then the ``asyncio.TimeoutError`` exception
                is raised. Set to 0 to disable timeout.
            readonly: Set to ``True`` when API is in read-only mode.
        """
        return self._run(self.connectAsync(
            host, port, clientId, timeout, readonly))

    def disconnect(self):
        """
        Disconnect from a TWS or IB gateway application.
        This will clear all session state.
        """
        if not self.client.isConnected():
            return
        stats = self.client.connectionStats()
        self._logger.info(
            f'Disconnecting from {self.client.host}:{self.client.port}, '
            f'{util.formatSI(stats.numBytesSent)}B sent '
            f'in {stats.numMsgSent} messages, '
            f'{util.formatSI(stats.numBytesRecv)}B received '
            f'in {stats.numMsgRecv} messages, '
            f'session time {util.formatSI(stats.duration)}s.')
        self.client.disconnect()

    def isConnected(self) -> bool:
        """
        Is there is an API connection to TWS or IB gateway?
        """
        return self.client.isConnected()

    run = staticmethod(util.run)
    schedule = staticmethod(util.schedule)
    sleep = staticmethod(util.sleep)
    timeRange = staticmethod(util.timeRange)
    timeRangeAsync = staticmethod(util.timeRangeAsync)
    waitUntil = staticmethod(util.waitUntil)

    def _run(self, *awaitables: List[Awaitable]):
        return util.run(*awaitables, timeout=self.RequestTimeout)

    def waitOnUpdate(self, timeout: float = 0) -> bool:
        """
        Wait on any new update to arrive from the network.

        Args:
            timeout: Maximum time in seconds to wait.
                If 0 then no timeout is used.

        .. note::
            A loop with ``waitOnUpdate`` should not be used to harvest
            tick data from tickers, since some ticks can go missing.
            This happens when multiple updates occur almost simultaneously;
            The ticks from the first update are then cleared.
            Use events instead to prevent this.
        """
        if timeout:
            with suppress(asyncio.TimeoutError):
                util.run(asyncio.wait_for(self.updateEvent, timeout))
        else:
            util.run(self.updateEvent)
        return True

    def loopUntil(
            self, condition=None, timeout: float = 0) -> Iterator[object]:
        """
        Iterate until condition is met, with optional timeout in seconds.
        The yielded value is that of the condition or False when timed out.

        Args:
            condition: Predicate function that is tested after every network
            update.
            timeout: Maximum time in seconds to wait.
                If 0 then no timeout is used.
        """
        endTime = time.time() + timeout
        while True:
            test = condition and condition()
            if test:
                yield test
                return
            elif timeout and time.time() > endTime:
                yield False
                return
            else:
                yield test
            self.waitOnUpdate(endTime - time.time() if timeout else 0)

    def setTimeout(self, timeout: float = 60):
        """
        Set a timeout for receiving messages from TWS/IBG, emitting
        ``timeoutEvent`` if there is no incoming data for too long.

        The timeout fires once per connected session but can be set again
        after firing or after a reconnect.

        Args:
            timeout: Timeout in seconds.
        """
        self.wrapper.setTimeout(timeout)

    def portfolio(self) -> List[PortfolioItem]:
        """
        List of portfolio items of the default account.
        """
        account = self.wrapper.accounts[0]
        return [v for v in self.wrapper.portfolio[account].values()]

    def positions(self, account: str = '') -> List[Position]:
        """
        List of positions for the given account,
        or of all accounts if account is left blank.

        Args:
            account: If specified, filter for this account name.
        """
        if account:
            return list(self.wrapper.positions[account].values())
        else:
            return [v for d in self.wrapper.positions.values()
                    for v in d.values()]

    def trades(self) -> List[Trade]:
        """
        List of all order trades from this session.
        """
        return list(self.wrapper.trades.values())

    def openTrades(self) -> List[Trade]:
        """
        List of all open order trades.
        """
        return [v for v in self.wrapper.trades.values()
                if v.orderStatus.status not in OrderStatus.DoneStates]

    def orders(self) -> List[Order]:
        """
        List of all orders from this session.
        """
        return list(
            trade.order for trade in self.wrapper.trades.values())

    def openOrders(self) -> List[Order]:
        """
        List of all open orders.
        """
        return [trade.order for trade in self.wrapper.trades.values()
                if trade.orderStatus.status not in OrderStatus.DoneStates]

    def fills(self) -> List[Fill]:
        """
        List of all fills from this session.
        """
        return list(self.wrapper.fills.values())

    def executions(self) -> List[Execution]:
        """
        List of all executions from this session.
        """
        return list(fill.execution for fill in self.wrapper.fills.values())

    def ticker(self, contract: Contract) -> Ticker:
        """
        Get ticker of the given contract. It must have been requested before
        with reqMktData with the same contract object. The ticker may not be
        ready yet if called directly after :meth:`.reqMktData`.

        Args:
            contract: Contract to get ticker for.
        """
        return self.wrapper.tickers.get(id(contract))

    def tickers(self) -> List[Ticker]:
        """
        Get a list of all tickers.
        """
        return list(self.wrapper.tickers.values())

    def pendingTickers(self) -> List[Ticker]:
        """
        Get a list of all tickers that have pending ticks or domTicks.
        """
        return list(self.wrapper.pendingTickers)

    def reqTickers(
            self, *contracts: List[Contract],
            regulatorySnapshot: bool = False) -> List[Ticker]:
        """
        Request and return a list of snapshot tickers.
        The list is returned when all tickers are ready.

        This method is blocking.

        Args:
            contracts: Contracts to get tickers for.
            regulatorySnapshot: Request NBBO snapshots (may incur a fee).
        """
        return self._run(
            self.reqTickersAsync(
                *contracts, regulatorySnapshot=regulatorySnapshot))

    def placeOrder(self, contract: Contract, order: Order) -> Trade:
        """
        Place a new order or modify an existing order.
        Returns a Trade that is kept live updated with
        status changes, fills, etc.

        Args:
            contract: Contract to use for order.
            order: The order to be placed.
        """
        orderId = order.orderId or self.client.getReqId()
        self.client.placeOrder(orderId, contract, order)
        now = datetime.datetime.now(datetime.timezone.utc)
        key = self.wrapper.orderKey(
            self.wrapper.clientId, orderId, order.permId)
        trade = self.wrapper.trades.get(key)
        if trade:
            # this is a modification of an existing order
            assert trade.orderStatus.status not in OrderStatus.DoneStates
            logEntry = TradeLogEntry(now, trade.orderStatus.status, 'Modify')
            trade.log.append(logEntry)
            self._logger.info(f'placeOrder: Modify order {trade}')
            trade.modifyEvent.emit(trade)
            self.orderModifyEvent.emit(trade)
        else:
            # this is a new order
            order.clientId = self.wrapper.clientId
            order.orderId = orderId
            orderStatus = OrderStatus(status=OrderStatus.PendingSubmit)
            logEntry = TradeLogEntry(now, orderStatus.status, '')
            trade = Trade(
                contract, order, orderStatus, [], [logEntry])
            self.wrapper.trades[key] = trade
            self._logger.info(f'placeOrder: New order {trade}')
            self.newOrderEvent.emit(trade)
        return trade

    def cancelOrder(self, order: Order) -> Trade:
        """
        Cancel the order and return the Trade it belongs to.

        Args:
            order: The order to be canceled.
        """
        self.client.cancelOrder(order.orderId)
        now = datetime.datetime.now(datetime.timezone.utc)
        key = self.wrapper.orderKey(
            order.clientId, order.orderId, order.permId)
        trade = self.wrapper.trades.get(key)
        if trade:
            if not trade.isDone():
                status = trade.orderStatus.status
                if (status == OrderStatus.PendingSubmit and not order.transmit
                        or status == OrderStatus.Inactive):
                    newStatus = OrderStatus.Cancelled
                else:
                    newStatus = OrderStatus.PendingCancel
                logEntry = TradeLogEntry(now, newStatus, '')
                trade.log.append(logEntry)
                trade.orderStatus.status = newStatus
                self._logger.info(f'cancelOrder: {trade}')
                trade.cancelEvent.emit(trade)
                trade.statusEvent.emit(trade)
                self.cancelOrderEvent.emit(trade)
                self.orderStatusEvent.emit(trade)
                if newStatus == OrderStatus.Cancelled:
                    trade.cancelledEvent.emit(trade)
        else:
            self._logger.error(f'cancelOrder: Unknown orderId {order.orderId}')
        return trade

    def reqGlobalCancel(self):
        """
        Cancel all active trades including those placed by other
        clients or TWS/IB gateway.
        """
        self.client.reqGlobalCancel()
        self._logger.info(f'reqGlobalCancel')

    def reqAccountUpdates(self, account: str = ''):
        """
        This is called at startup - no need to call again.

        Request account and portfolio values of the account
        and keep updated. Returns when both account values and portfolio
        are filled.

        This method is blocking.

        Args:
            account: If specified, filter for this account name.
        """
        self._run(self.reqAccountUpdatesAsync(account))

    # now entering the parallel async universe

    async def connectAsync(
            self, host='127.0.0.1', port=7497, clientId=1,
            timeout=2, readonly=False):

        async def connect():
            self.wrapper.clientId = clientId
            await self.client.connectAsync(host, port, clientId, timeout)
            if not readonly and self.client.serverVersion() >= 150:
                await self.reqCompletedOrdersAsync(False)
            accounts = self.client.getAccounts()
            await asyncio.gather(
                self.reqAccountUpdatesAsync(accounts[0]),
                *(self.reqAccountUpdatesMultiAsync(a) for a in accounts),
                self.reqPositionsAsync(),
                self.reqExecutionsAsync())
            if clientId == 0:
                # autobind manual orders
                self.reqAutoOpenOrders(True)
            self._logger.info('Synchronization complete')
            self.connectedEvent.emit()

        if not self.isConnected():
            try:
                await asyncio.wait_for(connect(), timeout or None)
            except Exception:
                self.disconnect()
                raise
        else:
            self._logger.warn('Already connected')
        return self

    async def qualifyContractsAsync(self, *contracts):
        detailsLists = await asyncio.gather(
            *(self.reqContractDetailsAsync(c) for c in contracts))
        result = []
        for contract, detailsList in zip(contracts, detailsLists):
            if not detailsList:
                self._logger.error(f'Unknown contract: {contract}')
            elif len(detailsList) > 1:
                possibles = [details.contract for details in detailsList]
                self._logger.error(
                    f'Ambiguous contract: {contract}, '
                    f'possibles are {possibles}')
            else:
                c = detailsList[0].contract
                expiry = c.lastTradeDateOrContractMonth
                if expiry:
                    # remove time and timezone part as it will cause problems
                    expiry = expiry.split()[0]
                    c.lastTradeDateOrContractMonth = expiry
                if contract.exchange == 'SMART':
                    # overwriting 'SMART' exchange can create invalid contract
                    c.exchange = contract.exchange
                contract.update(**c.dict())
                result.append(contract)
        return result

    async def reqTickersAsync(self, *contracts, regulatorySnapshot=False):
        futures = []
        tickers = []
        for contract in contracts:
            reqId = self.client.getReqId()
            future = self.wrapper.startReq(reqId, contract)
            futures.append(future)
            ticker = self.wrapper.startTicker(reqId, contract, 'snapshot')
            tickers.append(ticker)
            self.client.reqMktData(
                reqId, contract, '', True, regulatorySnapshot, [])
        await asyncio.gather(*futures)
        for ticker in tickers:
            self.wrapper.endTicker(ticker, 'snapshot')
        return tickers

    def whatIfOrderAsync(self, contract, order):
        whatIfOrder = Order(**order.dict()).update(whatIf=True)
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId, contract)
        self.client.placeOrder(reqId, contract, whatIfOrder)
        return future

    def reqCurrentTimeAsync(self):
        future = self.wrapper.startReq('currentTime')
        self.client.reqCurrentTime()
        return future

    def reqAccountUpdatesAsync(self, account):
        future = self.wrapper.startReq('accountValues')
        self.client.reqAccountUpdates(True, account)
        return future

    def reqAccountUpdatesMultiAsync(self, account, modelCode=''):
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        self.client.reqAccountUpdatesMulti(reqId, account, modelCode, False)
        return future

    def reqAccountSummaryAsync(self):
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        tags = (
            'AccountType,NetLiquidation,TotalCashValue,SettledCash,'
            'AccruedCash,BuyingPower,EquityWithLoanValue,'
            'PreviousEquityWithLoanValue,GrossPositionValue,ReqTEquity,'
            'ReqTMargin,SMA,InitMarginReq,MaintMarginReq,AvailableFunds,'
            'ExcessLiquidity,Cushion,FullInitMarginReq,FullMaintMarginReq,'
            'FullAvailableFunds,FullExcessLiquidity,LookAheadNextChange,'
            'LookAheadInitMarginReq,LookAheadMaintMarginReq,'
            'LookAheadAvailableFunds,LookAheadExcessLiquidity,'
            'HighestSeverity,DayTradesRemaining,Leverage,$LEDGER:ALL')
        self.client.reqAccountSummary(reqId, 'All', tags)
        return future

    def reqOpenOrdersAsync(self):
        future = self.wrapper.startReq('openOrders')
        self.client.reqOpenOrders()
        return future

    def reqAllOpenOrdersAsync(self):
        future = self.wrapper.startReq('openOrders')
        self.client.reqAllOpenOrders()
        return future

    def reqCompletedOrdersAsync(self, apiOnly):
        future = self.wrapper.startReq('completedOrders')
        self.client.reqCompletedOrders(apiOnly)
        return future

    def reqExecutionsAsync(self, execFilter=None):
        execFilter = execFilter or ExecutionFilter()
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        self.client.reqExecutions(reqId, execFilter)
        return future

    def reqPositionsAsync(self):
        future = self.wrapper.startReq('positions')
        self.client.reqPositions()
        return future

    def reqContractDetailsAsync(self, contract):
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId, contract)
        self.client.reqContractDetails(reqId, contract)
        return future

    async def reqMatchingSymbolsAsync(self, pattern):
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        self.client.reqMatchingSymbols(reqId, pattern)
        try:
            await asyncio.wait_for(future, 4)
            return future.result()
        except asyncio.TimeoutError:
            self._logger.error('reqMatchingSymbolsAsync: Timeout')

    async def reqMarketRuleAsync(self, marketRuleId):
        future = self.wrapper.startReq(f'marketRule-{marketRuleId}')
        try:
            self.client.reqMarketRule(marketRuleId)
            await asyncio.wait_for(future, 1)
            return future.result()
        except asyncio.TimeoutError:
            self._logger.error('reqMarketRuleAsync: Timeout')
