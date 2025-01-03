import multiprocessing as mp
from datetime import datetime, timedelta, time
from backtestTools.algoLogic import optIntraDayAlgoLogic
from backtestTools.histData import getFnoBacktestData


# Define a class algoLogic that inherits from optIntraDayAlgoLogic
class algoLogic(optIntraDayAlgoLogic):

    # Define a method 'run' to execute the algorithm
    def run(self, startDate, endDate, baseSym, indexSym):
        # Convert start and end dates to timestamps
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            # Fetch historical data for backtesting  
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            # Skip holidays
            if df is None:
                return
        except Exception as e:
            # Log an exception if data retrieval fails
            self.strategyLogger.info(
                f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        # Save the dataframe to a CSV file
        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{indexName}_{startDate.date()}.csv")

        # Get lot size from expiry data
        # lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])
        lotSize = int(self.fetchAndCacheExpiryData(
            startEpoch, baseSym)["LotSize"])

        # Strategy Parameters
        lastIndexTimeData = [0, 0]

        callsOrderTime = time(9, 45)
        callbOrderTime = time(9, 45)
        putsOrderTime = time(9, 45)
        putbOrderTime = time(9, 45)

        callT = False
        callO = False
        putT= False
        putO = False

        # Loop through each timestamp in the dataframe index
        for timeData in df.index:
            self.timeData = timeData
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) | (
                self.humanTime.time() > time(15, 30)
            ):
                continue


            # Update lastIndexTimeData
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)

            # Add self.strategyLogger and comments
            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")
                print(self.humanTime)

            # Update current price for open positions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            # Calculate and update PnL
            self.pnlCalculator()

            # Exit positions based on conditions
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if row["CurrentPrice"] <= (row["EntryPrice"] - (0.5 * row["EntryPrice"])) and row["PositionStatus"] == -1:
                        exitType = " Target Hit"
                        self.exitOrder(index, exitType)
                    elif row["CurrentPrice"] >= (row["EntryPrice"] + (0.3 * row["EntryPrice"])) and row["PositionStatus"] == -1:
                        exitType = "Stoploss Hit"
                        self.exitOrder(index, exitType)
                    elif row["CurrentPrice"] >= (row["EntryPrice"] + (0.5 * row["EntryPrice"])) and row["PositionStatus"] == 1:
                        exitType = " Target Hit"
                        self.exitOrder(index, exitType)  
                    elif row["CurrentPrice"] <= (row["EntryPrice"] - (0.3 * row["EntryPrice"])) and row["PositionStatus"] == 1:
                        exitType = "Stoploss Hit"
                        self.exitOrder(index, exitType)                                              
                    elif self.humanTime.time() >= time(15, 15):
                        exitType = "Time Up"
                        self.exitOrder(index, exitType)


            # Place orders based on conditions
            if (lastIndexTimeData[1] in df.index) & (self.humanTime.time() < time(15, 15)): 
                if (self.humanTime.time() >= callsOrderTime) & (callT == False):
                    callSym = self.getCallSym(
                        startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor= 2)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "SELL")
                    callT = True

                if (self.humanTime.time() >= putsOrderTime) & (putT == False):
                    putSym = self.getPutSym(
                        startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor= 2)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "SELL")
                    putT = True


                if (self.humanTime.time() >= callbOrderTime) & (callO == False):
                    callSym = self.getCallSym(
                        startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor= 6)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "BUY")
                    callO = True

                if (self.humanTime.time() >= putbOrderTime) & (putO == False):
                    putSym = self.getPutSym(
                        startEpoch, baseSym, df.at[lastIndexTimeData[1], "c"], otmFactor= 6)

                    try:
                        data = self.fetchAndCacheFnoHistData(
                            putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "BUY")
                    putO = True                  

        # Final PnL calculation and CSV export
        self.pnlCalculator()
        self.combinePnlCsv()


if __name__ == "__main__":
    start = datetime.now()

    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "straddle"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2021, 3, 1, 9, 15)
    endDate = datetime(2021, 3, 31, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    # Configure number of processes to be created
    maxConcurrentProcesses = 2
    processes = []

    # Start a loop from Start Date to End Date
    currentDate = startDate
    while currentDate <= endDate:
        # Define trading period for Current day
        startTime = datetime(
            currentDate.year, currentDate.month, currentDate.day, 9, 15, 0)
        endTime = datetime(
            currentDate.year, currentDate.month, currentDate.day, 15, 30, 0)

        p = mp.Process(target=algo.run, args=(
            startTime, endTime, baseSym, indexName))
        p.start()
        processes.append(p)

        if len(processes) >= maxConcurrentProcesses:
            for p in processes:
                p.join()
            processes = []

        currentDate += timedelta(days=1)

    end = datetime.now()
    print(f"Done. Ended in {end-start}.")