# Federal Election Commission MCP Server: Overview 

The Federal Election Commission (FEC) is an organization that helps protect the integirty of the campaign finance process in the US.

Visit https://www.fec.gov/ for information on the FEC and https://api.open.fec.gov/developers/ for information about their REST API.

# FEC MCP Tool

As a proof-of-concept, one REST call was wrapped in an MCP tool to allow LLMs to get access to the latest electronic filings with the FEC.

There are two files in this folder that support the MCP server:

- open_fec_client.py - this wraps various calls to the FEC REST API. Only **latest_filings** is exposed in the MCP server (there are other wrappers not exposed). 
- fec_info_mcp.py - this is an MCP server that implements one tool **latest_filings**

# Testing

The MCP server fec_info_mcp.py was tested with Claude Desktop and a special script in the ../examples folder called *crew_latest_fec_filings_summaries.py*.

# Sample Output from Claude Desktop

Here are the last 10 filings from the Federal Election Commission, all filed today (September 21, 2025):
Latest 10 FEC Filings (September 21, 2025)

TORCH & PITCHFORK PAC (C00687079)

Form Type: F3XN (Quarterly Report)
File Number: 1916377
Coverage Period: April 1 - June 30, 2025
Filed: 4:55 PM ET
PDF Report


DCOC PAC (C00870923)

Form Type: F1A (Statement of Organization Amendment)
File Number: 1916376
Amendment #4
Filed: 3:59 PM ET
PDF Report


GREEN PARTY OF CONNECTICUT (C00528356)

Form Type: F3XN (Monthly Report)
File Number: 1916375
Coverage Period: August 1-31, 2025
Filed: 3:51 PM ET
PDF Report

[Remainder truncated....]


# Sample Output from crew_latest_fec_filings_summaries.py

title: Latest FEC Filings Summaries
source_latest_timestamp: '2025-09-21T20:08:19Z'
items:
- committee_id: C00097865
  committee_name: NATSO INC. NATSO PAC
  paragraph: NATSO Inc. NATSO PAC is a political action committee (PAC) founded in 1986 [2]. It is connected to the Truck Stop and Travel Plaza Association [2]. The PAC is also the political action committee of NATSO, a trade association representing truck stops and travel plazas [5]. Information on the PAC can be found on the FEC website [3] and Bloomberg [1].
  refernces:
  - url: https://www.bloomberg.com/profile/company/0576074D:US
    title: Bloomberg Natso Inc Natso PAC - Company Profile and News - Bloomberg Markets
  - url: https://www.opensecrets.org/political-action-committees/natso-inc-natso-pac/C00097865/summary
    title: NATSO Inc NATSO PAC Profile - OpenSecrets
  - url: https://www.fec.gov/political-committee/NATSO+PAC/C00097865
    title: NATSO PAC - Truckstop Industry Political Action Committee | Federal ...
  - url: https://ballotpedia.org/NATSO_PAC
    title: NATSO PAC - Ballotpedia
  - url: https://www.influencewatch.org/political-action-committee/natso-inc-natso-pac/
    title: NATSO Inc. NATSO PAC - InfluenceWatch
- committee_id: C00909408
  committee_name: CHRISTOPHER HURT FOR CONGRESS
  paragraph: Christopher Hurt's campaign committee filed a Form 99 notifying the Federal Election Commission that it has not raised or spent $5,000 in contributions or expenditures [1].
  refernces:
  - url: https://docquery.fec.gov/pdf/301/202507169763557301/202507169763557301.pdf
    title: MISCELLANEOUS TEXT (FEC Form 99) CHRISTOPHER HURT ...
- committee_id: P80008261
  committee_name: Gallego, Patrick Paul Mister II
  paragraph: Patrick Paul is an American football offensive tackle for the Miami Dolphins of the NFL [1]. He played college football at Houston and was drafted by the Miami Dolphins in the second round [1]. More information can be found on NFL.com and Pro Football Focus [2][3].
  refernces:
  - url: https://en.wikipedia.org/wiki/Patrick_Paul
    title: Wikipedia Patrick Paul - Wikipedia
  - url: https://www.nfl.com/prospects/patrick-paul/32005041-5529-5943-2227-9290168a0246
    title: Patrick Paul Draft & Combine Profile | NFL.com
  - url: https://www.pff.com/nfl/players/patrick-paul/122515
    title: Patrick Paul - Pro Football Focus
- committee_id: C00458463
  committee_name: PORTMAN FOR SENATE COMMITTEE
  paragraph: The Portman for Senate Committee is a principal campaign committee [1]. It was registered with the FEC on January 22, 2009 [1]. Rob Portman served as a Republican U.S. Senator from Ohio from 2011 to 2023 [2]. He previously served in the House of Representatives [3].
  refernces:
  - url: https://www.fec.gov/data/committee/C00458463/
    title: PORTMAN FOR SENATE COMMITTEE
  - url: https://ballotpedia.org/Rob_Portman
    title: Rob Portman - Ballotpedia
  - url: https://en.wikipedia.org/wiki/Rob_Portman
    title: Rob Portman - Wikipedia
- committee_id: H6MI01275
  committee_name: Featherly, Zebulon Hart
  paragraph: Search results for Featherly, Zebulon Hart are sparse. The search instead turned up information on Heather T. Hart, an American visual artist [1]. Insufficient information was found to create a summary about Featherly, Zebulon Hart.
  refernces:
  - url: https://en.wikipedia.org/wiki/Heather_Hart
    title: Wikipedia Heather Hart - Wikipedia