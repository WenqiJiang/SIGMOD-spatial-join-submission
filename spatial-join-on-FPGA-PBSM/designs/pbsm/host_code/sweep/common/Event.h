#pragma once

#include "Region.h"

class Event
{
private:
  float y; // y coordinate
  bool bottom; // whether bottom (1) or top (0) boundary
  MBR *mbr; // pointer to the MBR
  bool dataset; // first trace (0) or second (1)

public:
  Event(float y, bool bottom, MBR *mbr, bool dataset) : y(y), bottom(bottom),
   mbr(mbr), dataset(dataset) {}

  float getY()
  {
    return y;
  }

  bool isBottom()
  {
    return bottom;
  }

  MBR* getMBR()
  {
    return mbr;
  }

  bool set()
  {
    return dataset;
  }

  bool operator<(const Event& other) const
  {
    if (y < other.y) {
        return true;
    } else if (y == other.y) {
        return bottom && !other.bottom;
    }
    return false;
  }
};