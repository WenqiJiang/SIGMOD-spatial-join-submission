#pragma once

#include <iostream>

class MBR
{
private:
  float _low0, _high0;
  float _low1, _high1;

public:
  MBR() {};

  MBR(float low0, float high0, float low1, float high1) : _low0(low0), _high0(high0),
  _low1(low1), _high1(high1) {}

  inline float get_low0() const
  {
    return _low0;
  }

  inline float get_high0() const
  {
    return _high0;
  }

  inline float get_low1() const
  {
    return _low1;
  }

  inline float get_high1() const
  {
    return _high1;
  }

  void set_low0(float low0)
  {
    _low0 = low0;
  }

  void set_high0(float high0)
  {
    _high0 = high0;
  }

  void set_low1(float low1)
  {
    _low1 = low1;
  }

  void set_high1(float high1)
  {
    _high1 = high1;
  }



  inline bool intersects(const MBR *const other) const
  {
    return ((_low0 <= other->get_high0()) && (_high0 >= other->get_low0()) &&
            (_low1 <= other->get_high1()) && (_high1 >= other->get_low1()));
    // return ((_low0 < other->get_high0()) && (_high0 > other->get_low0()) &&
    //         (_low1 < other->get_high1()) && (_high1 > other->get_low1()));
			
  }

  friend std::ostream &operator<<(std::ostream &out, const MBR &r)
  {
    return out << "low 0: " << r.get_low0() 
               << "\thigh 0: " << r.get_high0()
               << "\tlow1 : " << r.get_low1() 
               << "\thigh 1: " << r.get_high1();
  }
};
