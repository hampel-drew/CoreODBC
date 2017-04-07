// ~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
// Author:      Andrew Hampel
// Created On:  2017-04-02
// Dependecies: PSQLODBC.dll (32-bit), .NET 4.6.2
// ~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~


// Copyright 2017 Andrew Hampel

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.If not, see<http://www.gnu.org/licenses/>.


// TODO Create abstract class if additional CoreXxxx classes are written.
// TODO determine means of checking valid DSNs.

// POSSIBLE TODO Update property (and possibly method) names to fit C# coding standards?


using System;
using System.Data;
using System.Data.Odbc;


namespace corePgsql
{
    // <docstring>
    // Convenience class for performing CRUD operations with PostgreSQL.
    // </docstring>
    public class CorePgsql
    {
        public string sDSN;
        public OdbcDataReader oReader;

        private OdbcConnection oConnection;
        private bool bConnectionCreated = false;

        private const string sConnect = "DSN={0}";

        public CorePgsql(string sDSN)
        {
            this.sDSN = sDSN;
            this.oConnection = new OdbcConnection(String.Format(sConnect, this.sDSN));
            this.oConnection.Open();
            this.bConnectionCreated = true;
        }

        // <docstring>
        // Close the database connection and terminate the connection and reader objects.
        // </docstring>
        public void CloseConnection()
        {
            this.CloseReader();
            if (this.oConnection.State.ToString().ToLower() != "closed")
            {
                this.oConnection.Close();
            }

            this.oConnection = null;
            this.bConnectionCreated = false;
            this.oReader = null;
        }

        // <docstring>
        // Close the OdbcDataReader connection.
        // </docstring>
        public void CloseReader()
        {
            if (!this.oReader.IsClosed)
            {
                this.oReader.Close();
            }
        }

        // <docstring>
        // Execute sQuery and return the results as a DataTable.
        // 
        // This method is capable of handling both queries that return results (SELECT) and
        // queries that do not (INSERT, UPDATE, DELETE, etc.).
        //
        // This method will attempt to reconnected to the database if the connection is closed
        // when it is called.
        // </docstring>
        public DataTable Execute(string sQuery)
        {
            DataTable aResults = new DataTable();

            if (!this.IsConnected())
            {
                this.OpenConnection();
            }
            this.ExecuteReader(sQuery);

            if (this.oReader.HasRows)
            {
                aResults.Load(this.oReader);
            }
            this.oReader.Close();

            return aResults;
        }

        // <docstring>
        // Execute sQuery and populate oReader with the results.
        //
        // Use this method to execute a query and work directly with the oReader
        // OdbcDataReader object.
        //
        // The oReader object will remain connected until the CloseReader method is called.
        // </docstring>
        public void ExecuteReader(string sQuery)
        {
            OdbcCommand oCommand = new OdbcCommand(sQuery, this.oConnection);
            this.oReader = oCommand.ExecuteReader();

            oCommand = null;
        }

        // <docstring>
        // Return boolean indicator of connection status.
        // </docstring>
        public bool IsConnected()
        {
            bool bConnected;
            if (this.bConnectionCreated)
            {
                if (this.oConnection.State.ToString().ToLower() == "closed")
                {
                    bConnected = false;
                }
                else
                {
                    bConnected = true;
                }
            }
            else
            {
                bConnected = false;
            }

            return bConnected;
        }

        // <docstring>
        // Create or re-create the database connection.
        // </docstring>
        public void OpenConnection()
        {
            if (this.sDSN.Trim() != "")
            {
                if (!this.IsConnected())
                {
                    try
                    {
                        this.oConnection = new OdbcConnection(String.Format(
                            sConnect,
                            this.sDSN
                        ));
                        this.oConnection.Open();
                    }
                    catch (OdbcException eError)
                    {
                        throw new Exception(String.Format(
                            "An error occurred while opening the connection: {0}",
                            eError.Message
                        ));
                    }
                }
            }
            else
            {
                throw new Exception("Cannot connect to database with blank data source name.");
            }
        }
    }
}
// ~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
