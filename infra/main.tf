variable "project" {
  type    = string
  default = "kaggle-airline"
}

variable "location" {
  type    = string
  default = "westeurope"
}

resource "azurerm_resource_group" "rg" {
  name     = "${var.project}-rg"
  location = var.location
}

