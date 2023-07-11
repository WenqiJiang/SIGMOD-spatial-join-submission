class MBR:
    """
    Refer to Region2D: https://github.com/WenqiJiang/CPU-spatial-join/blob/main/include/2D/Region2D.h
    """

    low0 = None
    high0 = None
    low1 = None 
    high1 = None

    def __init__(self, low0 : float, high0 : float, low1 : float, high1 : float):
        self.low0 = low0
        self.high0 = high0
        self.low1 = low1
        self.high1 = high1

    def get_low0(self): return self.low0

    def get_high0(self): return self.high0

    def get_low1(self): return self.low1
    
    def get_high1(self): return self.high1

    def intersects(self, other):
        # whether the current MBR intersects another MBR
        # point intersect also counts
        if ((self.get_low0() <= other.get_high0() and self.get_high0() >= other.get_low0()) and 
            (self.get_low1() <= other.get_high1() and self.get_high1() >= other.get_low1())):
            return True
        else:
            return False
    
    def print(self):
        print("low 0: {}\thigh 0:{}\tlow 1: {}\thigh 1:{}".format(
            self.low0, self.high0, self.low1, self.high1))
