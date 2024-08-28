import sys
import math
import operator

if sys.version_info[0] >= 3:
    from functools import reduce


class statsException(Exception):
    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return (repr(self.parameter))


class stats(object):
    def __init__(self):
        self.items = []
        self.populationStdDev = None
        self.stdDev = None
        self.average = None
        self.total = None
        self.maxVal = None
        self.minVal = None
        self.median = None
        self.geometric_mean = None

    def clearArray(self):
        del (self.items[:])

    def reset(self):
        self.clearArray()
        self.populationStdDev = None
        self.StdDev = None
        self.average = None
        self.total = None
        self.maxVal = None
        self.minVal = None
        self.median = None
        self.geometric_mean = None

    def addValue(self, value):
        self.items.append(value)

    def calc_geometric_mean(self, values):
        try:
            # geo_mean = (reduce(operator.mul, values)) ** (1.0/len(values))
            geo_mean = (reduce(operator.mul, values)) ** (1.0 / len(values))
        except ValueError as e:
            geo_mean = None
        return geo_mean

    def getValueAtPercentile(self, percentile, linearInterpolate=False):
        value = None
        if len(self.items):
            percentile = percentile / 100.0
            value = -1.0
            tempList = self.items[0:len(self.items)]
            tempList.sort()
            if linearInterpolate:
                # We have to subtract one to give us the array index since arrays are zero indexed.
                offset = (percentile * (len(tempList) + 1)) - 1
                # Determine if the offset is an integer, if not we need to interpolate between the two points.
                val = offset % 1
                # If the modulus does not result in 0, the percentile requested falls in between 2 entries.
                if val != 0:
                    lowOffset = int(math.floor(offset))
                    hiOffset = int(math.ceil(offset))
                    if (lowOffset < len(tempList) and lowOffset > 0) and \
                            (hiOffset < len(tempList) and hiOffset > 0):
                        value = (tempList[lowOffset] + tempList[hiOffset]) / 2
                    else:
                        if hiOffset > len(tempList):
                            value = tempList[-1]
                        elif lowOffset < 0:
                            value = tempList[0]
                else:
                    value = tempList[int(offset)]
            else:
                offset = int(round((percentile * (len(tempList) + 1)) - 1, 1))
                value = tempList[offset]

            del (tempList[:])
        return value

    def doCalculations(self):
        self.total = None
        item_count = len(self.items)
        if item_count:
            self.items.sort()
            self.total = 0.0
            self.maxVal = max(self.items)
            self.minVal = min(self.items)
            self.average = sum(self.items) / item_count
            self.geometric_mean = self.calc_geometric_mean(self.items)

            if item_count % 2 == 0:
                ndx_lo = int(item_count / 2) - 1
                self.median = (self.items[ndx_lo] + self.items[ndx_lo + 1]) / 2.0
            else:
                med_ndx = int((item_count + 1) / 2)
                if med_ndx >= len(self.items):
                    med_ndx = len(self.items) - 1
                self.median = self.items[med_ndx]
            '''
            for val in self.items:
              self.total += val
              if self.maxVal == None or self.maxVal < val:
                self.maxVal = val
              ifself.minVal == None or self.minVal > val :
                self.minVal = val
            self.average = self.total / len(self.items)
            '''
            # Calculate standard deviation.
            deviationSum = 0.0
            for val in self.items:
                deviation = ((val - self.average) * (val - self.average))
                deviationSum += deviation
            if (item_count - 1) > 0:
                self.stdDev = math.sqrt(deviationSum / (item_count - 1))
            self.populationStdDev = math.sqrt(deviationSum / item_count)
            return True
        return False


class covariance(object):
    def __init__(self):
        self.x = stats()
        self.y = stats()

    def reset(self):
        self.x.reset()
        self.y.reset()

    def doCalculations(self, x, y):
        self.x.items = x
        self.y.items = y
        # Have to have the same count in each variable set.
        if (len(self.x.items) == len(self.y.items)):
            self.x.doCalculations()
            self.y.doCalculations()

            cnt = len(self.x.items)
            i = 0
            covariance = 0
            while i < cnt:
                dx = self.x.items[i] - self.x.average
                dy = self.y.items[i] - self.y.average
                covariance += (dx * dy)
                i += 1
            if (cnt > 0):
                covariance = covariance / cnt

            return (covariance)
        else:
            raise statsException("Arrays must be the same length.")


class correlation(covariance):
    def __init__(self):
        covariance.__init__(self)
        self.coefficient = None

    def doCalculations(self, x, y, type="pearson"):
        try:
            cov = covariance.doCalculations(self, x, y)
            correlation = -9999
            if (self.x.populationStdDev != None and self.x.populationStdDev != 0 and
                    self.y.populationStdDev != None and self.y.populationStdDev != 0):
                correlation = cov / (self.x.populationStdDev * self.y.populationStdDev)
            return (correlation)
        except Exception as e:
            print(e)


class vectorMagDir(object):

    def calcMagAndDir(self, x, y, positiveDegrees=True):
        magnitude = math.hypot(x, y)
        angle = math.atan2(y, x)
        # return(magnitude,angle)
        if (positiveDegrees):
            angle = 180 / math.pi * angle
            angle = 90 - angle;
            # only return positive degrees
            if (angle < 0):
                angle = 360 + angle;
        return (magnitude, angle)

    def calcVector(self, speed, direction):
        eastComp = speed * math.sin(math.radians(direction))
        northComp = speed * math.cos(math.radians(direction))

        return (eastComp, northComp)


def calcAvgSpeedAndDir(speed_dir_tuples):
    spdAvg = None
    dirAvg = None
    vectObj = vectorMagDir()

    # Take the tuples for speed and direction and get the vector components
    speed_dir_components = [vectObj.calcVector(u_v[0], u_v[1]) for u_v in speed_dir_tuples]

    # If we have speed and direction vectors, calc the averages.
    if len(speed_dir_components):
        east_avg = sum(v[0] for v in speed_dir_components) / float(len(speed_dir_components))
        north_avg = sum(v[1] for v in speed_dir_components) / float(len(speed_dir_components))
        # Calculate average with speed and direction components.
        spdAvg, dirAvg = vectObj.calcMagAndDir(east_avg, north_avg)

    return (spdAvg, dirAvg)


def calcAvgSpeedAndDirV2(speed_dir_tuples):
    spdAvg = None
    dirAvg = None
    vectObj = vectorMagDir()

    # Take the tuples for speed and direction and get the vector components
    speed_dir_components = [vectObj.calcVector(u_v[0], u_v[1]) for u_v in speed_dir_tuples]

    # If we have speed and direction vectors, calc the averages.
    if len(speed_dir_components):
        east_avg = sum(v[0] for v in speed_dir_components) / float(len(speed_dir_components))
        north_avg = sum(v[1] for v in speed_dir_components) / float(len(speed_dir_components))
        # Calculate average with speed and direction components.
        spdAvg, dirAvg = vectObj.calcMagAndDir(east_avg, north_avg)

    return ({'scalar': (spdAvg, dirAvg),
             'vector': (east_avg, north_avg)})
