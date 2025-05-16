"""
Script para extrair dados específicos da aba EFEITO de múltiplos arquivos PCAT
e consolidar em uma única planilha organizada por anos.
"""

import logging
import pandas as pd
import os
from typing import Dict, List, Tuple
from dataclasses import dataclass
from pathlib import Path


# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('efeito_extraction.log'),
        logging.StreamHandler()
    ]
)

@dataclass
class ExtractionConfig:
    """Configuração para extração de dados dos arquivos PCAT."""
    sheet_name: str = 'EFEITO'
    start_col: str = 'AI'
    end_col: str = 'AV'
    start_row: int = 1
    end_row: int = 11
    years: List[int] = None
    
    def __post_init__(self):
        if self.years is None:
            self.years = list(range(2014, 2025))


class PCATEffectExtractor:
    """Classe para extrair e processar dados da aba EFEITO dos arquivos PCAT."""
    
    def __init__(self, config: ExtractionConfig):
        """
        Inicializa o extrator de dados.
        
        Args:
            config: Configuração para extração de dados
        """
        self.config = config
        self.output_folder = os.path.join(os.getcwd(), "resultados_efeito")
        
        # Lista de tuplas (caminho_completo, descrição) dos arquivos PCAT
        self.files_to_process = [
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT_Cemar_2014.xlsx", "PCAT Maranhão 2014"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT_Cemar_2015.xlsx", "PCAT Maranhão 2015"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT_Cemar_2016.xlsx", "PCAT Maranhão 2016"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT_Cemar_2017.xlsx", "PCAT Maranhão 2017"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT_Cemar_2018 .xlsx", "PCAT Maranhão 2018"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT_Cemar_2019.xlsx", "PCAT Maranhão 2019"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT_Cemar_2020 V02.xlsx", "PCAT Maranhão 2020"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT Cemar 2021 V02.xlsx", "PCAT Maranhão 2021"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT Cemar 2022 V02.xlsx", "PCAT Maranhão 2022"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT Equatorial MA 2023 V02.xlsx", "PCAT Maranhão 2023"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT Equatorial MA 2024 V02.xlsx", "PCAT Maranhão 2024"),
            (r"C:\Users\Hiden Number\Desktop\PCATs\PCAT Equatorial MA 2025 V02.xlsx", "PCAT Maranhão 2025")
        ]
        # Dicionário para armazenar os dados por ano
        self.data_by_year = {year: [] for year in self.config.years}

    def _create_output_directory(self) -> None:
        """Cria diretório de saída se não existir."""
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
            logging.info(f"Diretório criado: {self.output_folder}")

    def extract_data_from_file(self, file_info: Tuple[str, str]) -> pd.DataFrame:
        """
        Extrai dados específicos de um arquivo PCAT.
        
        Args:
            file_info: Tupla com (caminho_do_arquivo, descrição)
            
        Returns:
            DataFrame com os dados extraídos ou None em caso de erro
        """
        file_path, description = file_info
        try:
            # Lê a aba específica do arquivo Excel
            df = pd.read_excel(
                file_path,
                sheet_name=self.config.sheet_name,
                usecols=f"{self.config.start_col}:{self.config.end_col}",
                nrows=self.config.end_row - self.config.start_row + 1,
                skiprows=self.config.start_row - 1
            )
            
            # Adiciona informações de identificação
            df.insert(0, 'Arquivo_Origem', description)
            df.insert(1, 'Caminho_Arquivo', file_path)
            
            return df
            
        except Exception as e:
            logging.error(f"Erro ao processar {description} ({file_path}): {str(e)}")
            return None

    def process_files(self) -> None:
        """Processa todos os arquivos PCAT e organiza os dados por ano."""
        for file_info in self.files_to_process:
            file_path, description = file_info
            if not file_path:  # Pula arquivos sem caminho definido
                continue
                
            if not os.path.exists(file_path):
                logging.error(f"Arquivo não encontrado: {file_path}")
                continue

            logging.info(f"Processando arquivo: {description}")
            df = self.extract_data_from_file(file_info)
            
            if df is not None:
                # Identifica o ano baseado no nome do arquivo e adiciona ao dicionário correspondente
                for year in self.config.years:
                    if str(year) in file_path:
                        self.data_by_year[year].append(df)
                        logging.info(f"Dados de {description} adicionados ao ano {year}")
                        break

    def save_consolidated_file(self) -> None:
        """Salva todos os dados em um único arquivo Excel com múltiplas abas."""
        output_file = os.path.join(self.output_folder, "PCAT_Efeito_Consolidado.xlsx")
        
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                for year in self.config.years:
                    if self.data_by_year[year]:
                        # Concatena todos os DataFrames do ano
                        df_year = pd.concat(self.data_by_year[year], ignore_index=True)
                        # Salva na aba correspondente
                        df_year.to_excel(writer, sheet_name=str(year), index=False)
                        logging.info(f"Dados do ano {year} salvos com sucesso")
                    else:
                        logging.warning(f"Nenhum dado encontrado para o ano {year}")
                        
            logging.info(f"Arquivo consolidado salvo em: {output_file}")
            
        except Exception as e:
            logging.error(f"Erro ao salvar arquivo consolidado: {str(e)}")

    def run(self) -> None:
        """Executa todo o processo de extração e consolidação."""
        logging.info("Iniciando processamento dos arquivos PCAT")
        
        # Verifica se há arquivos para processar
        if not any(path for path, _ in self.files_to_process):
            logging.error("Nenhum arquivo configurado para processamento")
            return
            
        self._create_output_directory()
        self.process_files()
        self.save_consolidated_file()
        
        logging.info("Processamento concluído com sucesso!")


if __name__ == "__main__":
    try:
        config = ExtractionConfig()
        extractor = PCATEffectExtractor(config)
        extractor.run()
    except Exception as e:
        logging.critical(f"Erro fatal na execução do programa: {str(e)}")
