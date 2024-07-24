# Historical Music Data Extraction and Analysis Tool

## Overview
This project provides a set of tools for extracting, processing, and analyzing historical music data from digitized archives. While initially developed for Swedish newspaper archives, the methodology is designed to be adaptable for various types of music-related historical data.

## Features
- Data extraction from digital archives (currently optimized for KB - the National Library of Sweden)
- Text processing and analysis of historical music mentions
- Integration with OpenAI's GPT models for advanced content analysis
- Flexible data output formats (Excel, JSON) for further research and visualization

## Components
1. `1_Download_from_KB`: Extracts data from the KB API
2. `2_LLM_Data_Processing`: Processes data from KB using LLM
3. `3_AccuracyTesting`: Compares data with existing human-made data, generating result score based on accuracy.
4. `KBDownloader.py`: Libraries for working with KB API Data
5. `config.yaml`: Config file for coordinating between the different scripts

## Getting Started1
1. Clone this repository
2. Install required dependencies (see `requirements.txt`)
3. Set up API keys for OpenAI
4. Adjust search parameters in `KBDownloader.py` for your specific research needs
5. Run the extraction and processing scripts
6. Analyze the output data using your preferred tools

## Expanding to Other Music Data Sources
While this tool is currently configured for Swedish newspaper archives, its modular design allows for adaptation to other historical music data sources. Potential expansions include:

- Concert programs and reviews
- Musical scores and sheet music collections
- Historical music periodicals and magazines
- Personal letters and diaries of musicians

To adapt the tool for new data sources, modify the data extraction and processing modules to match the structure and API of the target archive or database.

## Contributing
Contributions to expand and improve this tool are welcome. Please submit pull requests or open issues for bugs, feature requests, or suggestions.

## License
[Insert your chosen license here]

## Acknowledgments
- KB (National Library of Sweden) for providing access to their digital archives
- OpenAI for their powerful language models

## Contact
[Your contact information or preferred method of contact]
