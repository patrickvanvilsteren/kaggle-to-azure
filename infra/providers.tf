terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0" # any 3.x version
    }
  }
}

provider "azurerm" {
  features {}
}
