using GeoJSON.Net.Geometry;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace VATMap_Kafka
{
    /// <summary>
    /// Position for a client consumed from Kafka
    /// </summary>
    internal class PilotPosition
    {
        /// <summary>
        /// Client callsign
        /// </summary>
        [JsonProperty]
        internal string Callsign { get; set; }

        /// <summary>
        /// Client altitude
        /// </summary>
        [JsonProperty]
        internal int Altitude { get; set; }

        /// <summary>
        /// GeoJson Position
        /// </summary>
        [JsonProperty]
        internal string Position { get; set; }


        /// <summary>
        /// Constructs a PilotPosition object
        /// </summary>
        /// <param name="callsign">Callsign of client</param>
        /// <param name="altitude">Altitude of client</param>
        /// <param name="jsonPoint">GeoJson point of client</param>
        internal PilotPosition(string callsign, int altitude, Point jsonPoint)
        {
            this.Callsign = callsign;
            this.Altitude = altitude;
            this.Position = JsonConvert.SerializeObject(jsonPoint);
        }
    }
}
