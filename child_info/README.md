2. children hotspot information

child_id | hotspot (latitude, longitude) | hit_cnt | included mac_list | excluded mac_list | time_list | provider_list

- empty filtering
- child_id, hit_cnt ascending ordering
- hotspot is clustered within 500M 
- included mac_list has mac_list which is close to hotspot within 300M



```python
# python code
def distanceTo(coords):
  lat1, long1 = coords[0]
  lat2, long2 = coords[1]
  radius = 6371

  dlat = math.radians(lat2 - lat1)
  dlong = math.radians(long2 - long1)
  a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
      * math.cos(math.radians(lat2)) * math.sin(dlong/2) * math.sin(dlong/2)
  c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
  d = radius * c

  return d
```
```scala
// scala code
import java.util.Date
import java.text.SimpleDateFormat

object TS2Date{
  def main(args: Array[AString]) : Unit = {
    val ts = 1504561650141L // 13 digit
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    
    val date = df.format(ts)
    println(date)
  }
}
```


